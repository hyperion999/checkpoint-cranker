/**
 * Checkpoint Cranker for Dark Matter
 * 
 * Railway-compatible service that automatically checkpoints miners after rounds end.
 * This allows users to claim all accumulated rewards with a single signature.
 * 
 * Optimized for Helius RPC with batch processing and priority fees.
 * 
 * Environment Variables:
 *   SOLANA_RPC_URL    - Solana RPC endpoint (required) - use Helius for best performance
 *   PRIVATE_KEY       - Cranker wallet private key as JSON array (required)
 *   PROGRAM_ID        - Dark Matter program ID (required)
 *   DRY_RUN           - Set to "1" for dry run mode (optional)
 *   POLL_INTERVAL_MS  - Polling interval in ms (default: 5000)
 *   BATCH_SIZE        - Number of parallel transactions (default: 10)
 *   PRIORITY_FEE      - Priority fee in microlamports (default: 50000)
 */

// Polyfill for Node.js < 19 (crypto not globally available)
import { webcrypto } from 'crypto';
if (typeof globalThis.crypto === 'undefined') {
  (globalThis as any).crypto = webcrypto;
}

import { 
  Connection, 
  Keypair, 
  PublicKey, 
  SystemProgram,
  TransactionMessage,
  VersionedTransaction,
  ComputeBudgetProgram
} from "@solana/web3.js";
import * as anchor from "@coral-xyz/anchor";

// Load environment variables
const SOLANA_RPC_URL = process.env.SOLANA_RPC_URL;
const PRIVATE_KEY = process.env.PRIVATE_KEY;
const PROGRAM_ID_STR = process.env.PROGRAM_ID;
const DRY_RUN = process.env.DRY_RUN === "1";
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || "5000");
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "10");
const PRIORITY_FEE = parseInt(process.env.PRIORITY_FEE || "50000"); // microlamports

if (!SOLANA_RPC_URL) throw new Error("SOLANA_RPC_URL is required");
if (!PRIVATE_KEY) throw new Error("PRIVATE_KEY is required");
if (!PROGRAM_ID_STR) throw new Error("PROGRAM_ID is required");

const PROGRAM_ID = new PublicKey(PROGRAM_ID_STR);

// Parse private key (supports JSON array format)
function loadKeypair(privateKeyStr: string): Keypair {
  try {
    const parsed = JSON.parse(privateKeyStr);
    return Keypair.fromSecretKey(Uint8Array.from(parsed));
  } catch {
    throw new Error("PRIVATE_KEY must be a JSON array of numbers");
  }
}

// Seeds
const BOARD_SEED = Buffer.from("board");
const ROUND_SEED = Buffer.from("round");
const MINER_SEED = Buffer.from("miner");

const NUM_TILES = 15;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const toBytes = (value: number) => {
  const buf = Buffer.alloc(8);
  buf.writeBigUInt64LE(BigInt(value));
  return buf;
};

const deriveBoardPda = () =>
  PublicKey.findProgramAddressSync([BOARD_SEED], PROGRAM_ID)[0];

const deriveRoundPda = (roundId: number) =>
  PublicKey.findProgramAddressSync([ROUND_SEED, toBytes(roundId)], PROGRAM_ID)[0];

function decodeBoard(data: Buffer) {
  let offset = 8;
  const roundId = Number(data.readBigUInt64LE(offset)); offset += 8;
  const startSlot = Number(data.readBigUInt64LE(offset)); offset += 8;
  const endSlot = Number(data.readBigUInt64LE(offset));
  return { roundId, startSlot, endSlot };
}

function decodeMiner(data: Buffer) {
  let offset = 8;
  const authority = new PublicKey(data.slice(offset, offset + 32)); offset += 32;
  offset += NUM_TILES * 8 * 2; // Skip deployed and cumulative arrays
  const roundId = Number(data.readBigUInt64LE(offset)); offset += 8;
  const checkpointId = Number(data.readBigUInt64LE(offset)); offset += 8;
  const checkpointFee = Number(data.readBigUInt64LE(offset)); offset += 8;
  const rewardsSol = Number(data.readBigUInt64LE(offset)); offset += 8;
  const rewardsDark = Number(data.readBigUInt64LE(offset));
  return { authority, roundId, checkpointId, checkpointFee, rewardsSol, rewardsDark };
}

function decodeRound(data: Buffer) {
  let offset = 8;
  const id = Number(data.readBigUInt64LE(offset)); offset += 8;
  offset += NUM_TILES * 8 * 2; // Skip deployed and count arrays
  const winningTile = data.readUInt8(offset);
  return { id, winningTile };
}

// Get all miners in one RPC call
async function getAllMiners(connection: Connection): Promise<{ pubkey: PublicKey; data: ReturnType<typeof decodeMiner> }[]> {
  // Miner account size
  const MINER_SIZE = 8 + 32 + (NUM_TILES * 8) + (NUM_TILES * 8) + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 1;
  
  const accounts = await connection.getProgramAccounts(PROGRAM_ID, {
    filters: [{ dataSize: MINER_SIZE }],
  });
  
  const miners: { pubkey: PublicKey; data: ReturnType<typeof decodeMiner> }[] = [];
  
  for (const account of accounts) {
    try {
      const data = decodeMiner(account.account.data as Buffer);
      if (!data.authority.equals(PublicKey.default)) {
        miners.push({ pubkey: account.pubkey, data });
      }
    } catch {
      // Not a valid miner account, skip
    }
  }
  
  return miners;
}

// Batch fetch round accounts
async function batchGetRounds(
  connection: Connection,
  roundIds: number[]
): Promise<Map<number, ReturnType<typeof decodeRound> | null>> {
  const uniqueRoundIds = [...new Set(roundIds)];
  const roundPdas = uniqueRoundIds.map(id => deriveRoundPda(id));
  const roundInfos = await connection.getMultipleAccountsInfo(roundPdas);
  
  const result = new Map<number, ReturnType<typeof decodeRound> | null>();
  
  for (let i = 0; i < uniqueRoundIds.length; i++) {
    const roundId = uniqueRoundIds[i];
    const info = roundInfos[i];
    if (info) {
      try {
        result.set(roundId, decodeRound(info.data as Buffer));
      } catch {
        result.set(roundId, null);
      }
    } else {
      result.set(roundId, null);
    }
  }
  
  return result;
}

interface MinerToCheckpoint {
  pubkey: PublicKey;
  data: ReturnType<typeof decodeMiner>;
  roundPda: PublicKey;
}

// Build checkpoint transaction with priority fee
async function buildCheckpointTx(
  program: anchor.Program,
  connection: Connection,
  signer: Keypair,
  miner: MinerToCheckpoint,
  boardPda: PublicKey
): Promise<VersionedTransaction> {
  // Build the checkpoint instruction
  const ix = await program.methods
    .checkpoint()
    .accounts({
      signer: signer.publicKey,
      board: boardPda,
      round: miner.roundPda,
      miner: miner.pubkey,
      systemProgram: SystemProgram.programId,
    })
    .instruction();
  
  // Add priority fee instructions
  const priorityFeeIx = ComputeBudgetProgram.setComputeUnitPrice({
    microLamports: PRIORITY_FEE,
  });
  
  const computeUnitsIx = ComputeBudgetProgram.setComputeUnitLimit({
    units: 100_000, // checkpoint is lighter than auto_deploy
  });
  
  // Get recent blockhash
  const { blockhash } = await connection.getLatestBlockhash('confirmed');
  
  // Build versioned transaction
  const messageV0 = new TransactionMessage({
    payerKey: signer.publicKey,
    recentBlockhash: blockhash,
    instructions: [priorityFeeIx, computeUnitsIx, ix],
  }).compileToV0Message();
  
  const tx = new VersionedTransaction(messageV0);
  tx.sign([signer]);
  
  return tx;
}

// Send transaction with retry logic
async function sendTxWithRetry(
  connection: Connection,
  tx: VersionedTransaction,
  authority: string,
  maxRetries: number = 3
): Promise<boolean> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const sig = await connection.sendTransaction(tx, {
        skipPreflight: false,
        maxRetries: 2,
      });
      
      // Wait for confirmation with timeout
      const confirmation = await connection.confirmTransaction({
        signature: sig,
        blockhash: tx.message.recentBlockhash,
        lastValidBlockHeight: (await connection.getLatestBlockhash()).lastValidBlockHeight,
      }, 'confirmed');
      
      if (confirmation.value.err) {
        console.error(`  ✗ Failed for ${authority}...: ${JSON.stringify(confirmation.value.err)}`);
        return false;
      }
      
      console.log(`  ✓ Checkpointed ${authority}...`);
      return true;
    } catch (err: any) {
      const errMsg = err.message || '';
      
      // Rate limit - wait and retry
      if (errMsg.includes('429') || errMsg.includes('rate') || errMsg.includes('Too many')) {
        console.log(`  Rate limited, waiting 1s...`);
        await sleep(1000);
        continue;
      }
      
      // Already checkpointed - treat as success
      if (errMsg.includes('AlreadyCheckpointed') || errMsg.includes('6005')) {
        console.log(`  ✓ Already checkpointed ${authority}...`);
        return true;
      }
      
      console.error(`  ✗ Failed for ${authority}...: ${errMsg}`);
      return false;
    }
  }
  
  return false;
}

// Process miners in batches
async function processBatch(
  program: anchor.Program,
  connection: Connection,
  signer: Keypair,
  miners: MinerToCheckpoint[],
  boardPda: PublicKey
): Promise<{ checkpointed: number; errors: number }> {
  let checkpointed = 0;
  let errors = 0;
  
  if (DRY_RUN) {
    for (const miner of miners) {
      console.log(`  [DRY RUN] Would checkpoint ${miner.data.authority.toBase58().slice(0, 8)}... for round ${miner.data.roundId}`);
    }
    return { checkpointed: miners.length, errors: 0 };
  }
  
  // Build all transactions first
  const txPromises = miners.map(miner => 
    buildCheckpointTx(program, connection, signer, miner, boardPda)
      .then(tx => ({ tx, miner }))
      .catch(err => {
        console.error(`  ✗ Failed to build tx for ${miner.data.authority.toBase58().slice(0, 8)}...: ${err.message}`);
        return null;
      })
  );
  
  const txResults = await Promise.all(txPromises);
  const validTxs = txResults.filter((r): r is { tx: VersionedTransaction; miner: MinerToCheckpoint } => r !== null);
  
  // Send all transactions in parallel
  const sendPromises = validTxs.map(({ tx, miner }) => 
    sendTxWithRetry(connection, tx, miner.data.authority.toBase58().slice(0, 8))
  );
  
  const results = await Promise.all(sendPromises);
  
  for (const success of results) {
    if (success) {
      checkpointed++;
    } else {
      errors++;
    }
  }
  
  // Count build failures as errors
  errors += txResults.filter(r => r === null).length;
  
  return { checkpointed, errors };
}

async function runCheckpointCycle(
  program: anchor.Program,
  connection: Connection,
  signer: Keypair,
  boardPda: PublicKey
): Promise<{ checkpointed: number; skipped: number; errors: number }> {
  let totalCheckpointed = 0;
  let totalSkipped = 0;
  let totalErrors = 0;

  const boardInfo = await connection.getAccountInfo(boardPda);
  if (!boardInfo) {
    console.log("Program not initialized.");
    return { checkpointed: 0, skipped: 0, errors: 0 };
  }
  
  const boardData = decodeBoard(boardInfo.data as Buffer);
  const miners = await getAllMiners(connection);
  
  if (miners.length === 0) {
    return { checkpointed: 0, skipped: 0, errors: 0 };
  }
  
  // Find miners needing checkpoint
  const minersNeedingCheckpoint = miners.filter(m => {
    const isNeverCheckpointed = m.data.checkpointId > 1e18;
    const alreadyCheckpointed = m.data.checkpointId === m.data.roundId && !isNeverCheckpointed;
    const hasParticipated = m.data.roundId < boardData.roundId;
    return hasParticipated && !alreadyCheckpointed;
  });

  if (minersNeedingCheckpoint.length === 0) {
    return { checkpointed: 0, skipped: miners.length, errors: 0 };
  }

  // Batch fetch all round accounts needed
  const roundIds = minersNeedingCheckpoint.map(m => m.data.roundId);
  const roundDataMap = await batchGetRounds(connection, roundIds);
  
  // Filter to miners whose rounds are settled (winningTile != 255)
  const eligibleMiners: MinerToCheckpoint[] = [];
  
  for (const miner of minersNeedingCheckpoint) {
    const roundData = roundDataMap.get(miner.data.roundId);
    
    if (!roundData) {
      totalSkipped++;
      continue;
    }
    
    if (roundData.winningTile === 255) {
      // Round not settled yet
      totalSkipped++;
      continue;
    }
    
    eligibleMiners.push({
      pubkey: miner.pubkey,
      data: miner.data,
      roundPda: deriveRoundPda(miner.data.roundId),
    });
  }
  
  if (eligibleMiners.length === 0) {
    return { checkpointed: 0, skipped: totalSkipped + (minersNeedingCheckpoint.length - totalSkipped), errors: 0 };
  }

  console.log(`[${new Date().toISOString()}] Found ${eligibleMiners.length} miners ready for checkpoint`);
  
  // Process in batches
  for (let i = 0; i < eligibleMiners.length; i += BATCH_SIZE) {
    const batch = eligibleMiners.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(eligibleMiners.length / BATCH_SIZE);
    
    if (totalBatches > 1) {
      console.log(`  Processing batch ${batchNum}/${totalBatches} (${batch.length} miners)...`);
    }
    
    const result = await processBatch(program, connection, signer, batch, boardPda);
    
    totalCheckpointed += result.checkpointed;
    totalErrors += result.errors;
    
    // Small delay between batches to avoid rate limiting
    if (i + BATCH_SIZE < eligibleMiners.length) {
      await sleep(500);
    }
  }
  
  console.log(`  Summary: ${totalCheckpointed} checkpointed, ${totalSkipped} skipped, ${totalErrors} errors`);
  
  return { checkpointed: totalCheckpointed, skipped: totalSkipped, errors: totalErrors };
}

async function main() {
  const connection = new Connection(SOLANA_RPC_URL!, {
    commitment: "confirmed",
    confirmTransactionInitialTimeout: 60000,
  });
  const signer = loadKeypair(PRIVATE_KEY!);
  
  const wallet = new anchor.Wallet(signer);
  const provider = new anchor.AnchorProvider(connection, wallet, {
    preflightCommitment: "confirmed",
  });

  console.log("============================================================");
  console.log("DARK MATTER - Checkpoint Cranker (Batch Processing)");
  console.log("============================================================");
  console.log(`Program ID: ${PROGRAM_ID.toBase58()}`);
  console.log(`Cranker: ${signer.publicKey.toBase58()}`);
  console.log(`RPC: ${SOLANA_RPC_URL?.includes('helius') ? 'Helius' : SOLANA_RPC_URL?.slice(0, 50)}...`);
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Poll interval: ${POLL_INTERVAL_MS / 1000}s`);
  console.log(`Batch size: ${BATCH_SIZE}`);
  console.log(`Priority fee: ${PRIORITY_FEE} microlamports`);
  console.log("============================================================\n");

  // Load program IDL
  const idl = await anchor.Program.fetchIdl(PROGRAM_ID, provider);
  if (!idl) {
    console.error("ERROR: Could not fetch IDL. Is the program deployed?");
    process.exit(1);
  }
  const program = new anchor.Program(idl, provider);

  const boardPda = deriveBoardPda();
  let totalCheckpointed = 0;
  let totalErrors = 0;

  console.log("Starting checkpoint cranker loop...\n");

  while (true) {
    try {
      const { checkpointed, errors } = await runCheckpointCycle(program, connection, signer, boardPda);
      totalCheckpointed += checkpointed;
      totalErrors += errors;
    } catch (err: any) {
      console.error(`Error in checkpoint cycle: ${err.message}`);
    }

    await sleep(POLL_INTERVAL_MS);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

/**
 * Checkpoint Cranker for Dark Matter
 * 
 * Railway-compatible service that automatically checkpoints miners after rounds end.
 * This allows users to claim all accumulated rewards with a single signature.
 * 
 * Environment Variables:
 *   SOLANA_RPC_URL    - Solana RPC endpoint (required)
 *   PRIVATE_KEY       - Cranker wallet private key as JSON array (required)
 *   PROGRAM_ID        - Dark Matter program ID (required)
 *   DRY_RUN           - Set to "1" for dry run mode (optional)
 *   POLL_INTERVAL_MS  - Polling interval in ms (default: 5000)
 */

import { 
  Connection, 
  Keypair, 
  PublicKey, 
  SystemProgram
} from "@solana/web3.js";
import * as anchor from "@coral-xyz/anchor";

// Load environment variables
const SOLANA_RPC_URL = process.env.SOLANA_RPC_URL;
const PRIVATE_KEY = process.env.PRIVATE_KEY;
const PROGRAM_ID_STR = process.env.PROGRAM_ID;
const DRY_RUN = process.env.DRY_RUN === "1";
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || "5000");

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

async function checkpointMiner(
  program: anchor.Program,
  connection: Connection,
  signer: Keypair,
  minerPubkey: PublicKey,
  minerData: ReturnType<typeof decodeMiner>,
  boardPda: PublicKey,
  currentRoundId: number
): Promise<{ checkpointed: number; skipped: number; errors: number }> {
  let checkpointed = 0;
  let skipped = 0;
  let errors = 0;
  
  const roundToCheckpoint = minerData.roundId;
  
  // Already checkpointed?
  const isNeverCheckpointed = minerData.checkpointId > 1e18;
  if (minerData.checkpointId === roundToCheckpoint && !isNeverCheckpointed) {
    skipped++;
    return { checkpointed, skipped, errors };
  }
  
  // Round still active?
  if (roundToCheckpoint >= currentRoundId) {
    skipped++;
    return { checkpointed, skipped, errors };
  }
  
  // Get round account
  const roundPda = deriveRoundPda(roundToCheckpoint);
  const roundInfo = await connection.getAccountInfo(roundPda);
  
  if (!roundInfo) {
    console.log(`  Round ${roundToCheckpoint} not found, skipping`);
    skipped++;
    return { checkpointed, skipped, errors };
  }
  
  const roundData = decodeRound(roundInfo.data as Buffer);
  
  // Round not settled yet?
  if (roundData.winningTile === 255) {
    console.log(`  Round ${roundToCheckpoint} not settled, skipping`);
    skipped++;
    return { checkpointed, skipped, errors };
  }
  
  if (DRY_RUN) {
    console.log(`  [DRY RUN] Would checkpoint ${minerData.authority.toBase58().slice(0, 8)}... for round ${roundToCheckpoint}`);
    checkpointed++;
    return { checkpointed, skipped, errors };
  }
  
  try {
    await program.methods
      .checkpoint()
      .accounts({
        signer: signer.publicKey,
        board: boardPda,
        round: roundPda,
        miner: minerPubkey,
        systemProgram: SystemProgram.programId,
      })
      .rpc();
    
    console.log(`  ✓ Checkpointed ${minerData.authority.toBase58().slice(0, 8)}... for round ${roundToCheckpoint}`);
    checkpointed++;
    await sleep(200); // Rate limiting
  } catch (err: any) {
    console.error(`  ✗ Failed: ${err.message}`);
    errors++;
  }
  
  return { checkpointed, skipped, errors };
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

  console.log(`[${new Date().toISOString()}] Found ${minersNeedingCheckpoint.length} miners needing checkpoint`);
  
  for (const miner of minersNeedingCheckpoint) {
    const result = await checkpointMiner(
      program,
      connection,
      signer,
      miner.pubkey,
      miner.data,
      boardPda,
      boardData.roundId
    );
    
    totalCheckpointed += result.checkpointed;
    totalSkipped += result.skipped;
    totalErrors += result.errors;
  }
  
  if (totalCheckpointed > 0) {
    console.log(`  Summary: ${totalCheckpointed} checkpointed, ${totalSkipped} skipped, ${totalErrors} errors`);
  }
  
  return { checkpointed: totalCheckpointed, skipped: totalSkipped, errors: totalErrors };
}

async function main() {
  const connection = new Connection(SOLANA_RPC_URL!, "confirmed");
  const signer = loadKeypair(PRIVATE_KEY!);
  
  const wallet = new anchor.Wallet(signer);
  const provider = new anchor.AnchorProvider(connection, wallet, {
    preflightCommitment: "confirmed",
  });

  console.log("============================================================");
  console.log("DARK MATTER - Checkpoint Cranker");
  console.log("============================================================");
  console.log(`Program ID: ${PROGRAM_ID.toBase58()}`);
  console.log(`Cranker: ${signer.publicKey.toBase58()}`);
  console.log(`RPC: ${SOLANA_RPC_URL}`);
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Poll interval: ${POLL_INTERVAL_MS / 1000}s`);
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

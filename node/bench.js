import pkg from 'kafkajs';
const { Kafka, CompressionTypes, CompressionCodecs, logLevel } = pkg;
import SnappyCodec from 'kafkajs-snappy';
// import LZ4Codec from 'kafkajs-lz4';
import avro from 'avsc';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';

// Register compression codecs
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;
// CompressionCodecs[CompressionTypes.LZ4] = new LZ4Codec().codec;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ============================================================================
// Configuration
// ============================================================================

let TEXT_MESSAGES = [
  'Hello world',
  'Quick brown fox jumps over the lazy dog',
  'Kafka benchmarking message',
  'Avro vs JSON throughput test',
  'Message compression experiment',
];

const argv = yargs(hideBin(process.argv))
  .options({
    mode: { type: 'string', choices: ['json', 'avro'], demandOption: true },
    messages: { type: 'number', default: 100000 },
    batch: { type: 'number', default: 100 },
    compression: { type: 'string', choices: ['none', 'gzip', 'snappy', 'lz4'], default: 'gzip' },
    brokers: { type: 'string', default: 'localhost:9094' },
    topic: { type: 'string', default: '' },
    large: { type: 'boolean', default: false },
  })
  .parseSync();

// ============================================================================
// Helper Functions
// ============================================================================

function getCompressionType(name) {
  const map = {
    'gzip': CompressionTypes.GZIP,
    'snappy': CompressionTypes.Snappy,
    'lz4': CompressionTypes.LZ4,
    'none': CompressionTypes.None,
  };
  return map[name] || CompressionTypes.None;
}

function buildRecord(i) {
  return {
    id: String(i),
    ts_unix_ms: Date.now(),
    category: 'bench',
    value: Math.random(),
    message: TEXT_MESSAGES[i % TEXT_MESSAGES.length],
  };
}

function serializeRecord(record, mode, avroType) {
  if (mode === 'json') {
    return Buffer.from(JSON.stringify(record));
  } else {
    return avroType.toBuffer(record);
  }
}

function deserializeRecord(buffer, mode, avroType) {
  if (mode === 'json') {
    return JSON.parse(buffer.toString('utf8'));
  } else {
    return avroType.fromBuffer(buffer);
  }
}

// ============================================================================
// Main Benchmark
// ============================================================================

async function runBenchmark() {
  const { mode, messages: totalMessages, batch: batchSize, compression, brokers, large } = argv;

  const topic = argv.topic || (mode === 'json' ? 'bench-json' : 'bench-avro');
  const compressionType = getCompressionType(compression);

  if (large) {
    TEXT_MESSAGES = TEXT_MESSAGES.map(message => message.repeat(1000));
  }

  // Load Avro schema if needed
  let avroType = null;
  if (mode === 'avro') {
    const schemaPath = path.join(__dirname, '../schemas/event.avsc');
    const schemaJson = JSON.parse(fs.readFileSync(schemaPath, 'utf8'));
    avroType = avro.Type.forSchema(schemaJson);
  }

  // Setup Kafka client
  const kafka = new Kafka({
    brokers: brokers.split(','),
    logLevel: logLevel.NOTHING,
  });

  // ============================================================================
  // Step 1: Start Consumer (subscribe from LATEST, not beginning)
  // ============================================================================

  const consumer = kafka.consumer({
    groupId: `bench-node-${mode}-${Date.now()}`,
  });
  await consumer.connect();
  
  // Subscribe from latest - we only want NEW messages from this run
  await consumer.subscribe({ topic, fromBeginning: false });

  let consumedCount = 0;
  let totalMessageLength = 0;
  let consumeStart = null;
  let consumeEnd = null;
  let resolveConsumption;
  const consumptionComplete = new Promise(resolve => {
    resolveConsumption = resolve;
  });

  // Start consuming in background
  consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      
      // Mark consumption start time on first message
      if (consumeStart === null) {
        consumeStart = Date.now();
      }
      
      try {
        const record = deserializeRecord(message.value, mode, avroType);
        totalMessageLength += record.message.length;
        consumedCount++;

        // Check if we've consumed all messages
        if (consumedCount >= totalMessages) {
          consumeEnd = Date.now();
          resolveConsumption();
        }
      } catch (err) {
        // Skip malformed messages
      }
    },
  });

  // Give consumer time to subscribe and be ready
  await new Promise(resolve => setTimeout(resolve, 2000));

  // ============================================================================
  // Step 2: Produce Messages
  // ============================================================================

  const producer = kafka.producer();
  await producer.connect();

  const produceStart = Date.now();
  let producedCount = 0;
  let messageBatch = [];

  for (let i = 0; i < totalMessages; i++) {
    const record = buildRecord(i);
    const value = serializeRecord(record, mode, avroType);
    messageBatch.push({ value });

    // Send batch when full
    if (messageBatch.length >= batchSize) {
      await producer.send({ topic, compression: compressionType, messages: messageBatch });
      producedCount += messageBatch.length;
      messageBatch = [];
    }
  }

  // Send remaining messages
  if (messageBatch.length > 0) {
    await producer.send({ topic, compression: compressionType, messages: messageBatch });
    producedCount += messageBatch.length;
  }

  const produceEnd = Date.now();
  await producer.disconnect();

  // ============================================================================
  // Step 3: Wait for Consumption to Complete
  // ============================================================================

  await consumptionComplete;
  await consumer.disconnect();

  // ============================================================================
  // Step 4: Report Results
  // ============================================================================

  const produceDuration = (produceEnd - produceStart) / 1000;
  const produceRate = producedCount / produceDuration;
  const consumeDuration = consumeEnd && consumeStart ? (consumeEnd - consumeStart) / 1000 : 0;

  const result = {
    lang: 'node',
    mode,
    topic,
    produced: producedCount,
    durationSec: produceDuration,
    rate: produceRate,
    consumed: consumedCount,
    stringLenSum: totalMessageLength,
    consumeDurSec: consumeDuration,
  };

  console.log(JSON.stringify(result, null, 2));
  
  // Ensure process exits
  process.exit(0);
}

// ============================================================================
// Entry Point
// ============================================================================

runBenchmark().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
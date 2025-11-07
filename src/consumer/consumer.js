// src/consumer/consumer.js
import { Kafka } from 'kafkajs';
import { MongoClient } from 'mongodb';
import dotenv from 'dotenv';

dotenv.config();

// Kafka setup
const kafka = new Kafka({
  clientId: 'kafka-docker-app-client',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092'],
});

const consumer = kafka.consumer({ groupId: process.env.GROUP_ID || 'app-consumer-group' });

// MongoDB setup
const MONGODB_URI = process.env.MONGODB_URI;
const MONGO_DB_NAME = process.env.MONGO_DB_NAME || 'kafka_db';

if (!MONGODB_URI) {
  console.error('❌ MONGODB_URI not set. Please add it to your .env file.');
  process.exit(1);
}

let db;
async function connectMongo() {
  try {
    const client = new MongoClient(MONGODB_URI);
    await client.connect();
    db = client.db(MONGO_DB_NAME);
    console.log(`✅ Connected to MongoDB Atlas (db: ${MONGO_DB_NAME})`);
  } catch (err) {
    console.error('❌ MongoDB connection failed:', err);
    process.exit(1);
  }
}

async function startConsumer() {
  await connectMongo();
  await consumer.connect();

  const topics = (process.env.TOPICS
    ? process.env.TOPICS.split(',').map(t => t.trim())
    : [
        'age_analysis',
        'payments',
        'purchases',
        'sales_transactions',
        'customer',
        'products',
        'representative',
        'suppliers',
        'sales',
      ]);

  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: true });
    console.log(`🟢 Subscribed to topic: ${topic}`);
  }

  console.log(`🚀 Kafka Consumer connected to ${process.env.KAFKA_BROKER || 'kafka:9092'}`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value?.toString();
      console.log(`📩 [${topic}] offset ${message.offset}: ${value}`);

      let payload;
      try {
        payload = JSON.parse(value);
      } catch {
        payload = { raw: value };
      }

      try {
        const col = db.collection(topic);
        await col.insertOne({
          kafkaOffset: message.offset,
          partition,
          receivedAt: new Date(),
          payload,
        });
        console.log(`✅ Inserted message into MongoDB → ${topic}`);
      } catch (err) {
        console.error('❌ Mongo insert error:', err);
      }
    },
  });
}

process.on('SIGINT', async () => {
  console.log('🛑 Shutting down consumer...');
  await consumer.disconnect();
  process.exit(0);
});

startConsumer().catch(err => {
  console.error('💥 Consumer startup error:', err);
  process.exit(1);
});

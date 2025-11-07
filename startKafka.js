import Docker from "dockerode";
import { Kafka } from "kafkajs";
import dotenv from "dotenv";
dotenv.config();

const docker = new Docker();
const ZOOKEEPER_CONTAINER = "zookeeper";
const KAFKA_CONTAINER = "kafka";
const ZOOKEEPER_IMAGE = "confluentinc/cp-zookeeper:7.4.1";
const KAFKA_IMAGE = "confluentinc/cp-kafka:7.4.1";

const KAFKA_PORT = 29092; // host port
const ZOOKEEPER_PORT = 2181;

// Topics to create
const TOPICS = [
  "payments",
  "sales",
  "purchases",
  "customer",
  "products",
  "suppliers"
];

async function startContainer(name, image, ports, env) {
  try {
    const existing = await docker.listContainers({ all: true });
    for (const c of existing) {
      if (c.Names.includes("/" + name)) {
        console.log(`🧹 Removing old container: ${name}`);
        const container = docker.getContainer(c.Id);
        await container.remove({ force: true });
      }
    }

    console.log(`🚀 Starting ${name}...`);
    const container = await docker.createContainer({
      name,
      Image: image,
      HostConfig: { PortBindings: ports },
      Env: env,
    });
    await container.start();
    console.log(`✅ ${name} started`);
  } catch (err) {
    console.error(`❌ Failed to start ${name}:`, err.message);
  }
}

async function startZookeeper() {
  await startContainer(
    ZOOKEEPER_CONTAINER,
    ZOOKEEPER_IMAGE,
    { "2181/tcp": [{ HostPort: "2181" }] },
    ["ZOOKEEPER_CLIENT_PORT=2181", "ZOOKEEPER_TICK_TIME=2000"]
  );
}

async function startKafka() {
  await startContainer(
    KAFKA_CONTAINER,
    KAFKA_IMAGE,
    { "9092/tcp": [{ HostPort: "9092" }], "29092/tcp": [{ HostPort: "29092" }] },
    [
      "KAFKA_BROKER_ID=1",
      "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
      "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092",
      "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092",
      "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
      "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
      "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
    ]
  );
}

async function wait(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

async function createTopics() {
  console.log("🧠 Connecting to Kafka...");
  const kafka = new Kafka({ clientId: "topic-creator", brokers: ["localhost:29092"] });
  const admin = kafka.admin();
  await admin.connect();

  const existingTopics = await admin.listTopics();
  for (const topic of TOPICS) {
    if (!existingTopics.includes(topic)) {
      console.log(`📦 Creating topic: ${topic}`);
      await admin.createTopics({ topics: [{ topic }] });
    } else {
      console.log(`⚙️ Topic already exists: ${topic}`);
    }
  }

  await admin.disconnect();
  console.log("✅ All topics ready");
}

async function main() {
  await startZookeeper();
  console.log("🕓 Waiting for Zookeeper to initialize...");
  await wait(10000);

  await startKafka();
  console.log("🕓 Waiting for Kafka to initialize...");
  await wait(20000);

  await createTopics();

  console.log("🎯 Kafka and Zookeeper running. Starting consumer...");
  await import("./src/consumer/consumer.js");
}

main().catch((err) => console.error("Startup failed:", err));

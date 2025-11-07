import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: ['kafka:9092']
});

const producer = kafka.producer();

const run = async () => {
  await producer.connect();
  
  // Sending messages to the payments topic
  await producer.send({
    topic: 'payments',
    messages: [
      { value: 'Payment message 1' },
      { value: 'Payment message 2' }
    ],
  });

  // Sending messages to the sales topic
  await producer.send({
    topic: 'sales',
    messages: [
      { value: 'Sale message 1' },
      { value: 'Sale message 2' }
    ],
  });

  console.log('Messages sent successfully');
  await producer.disconnect();
};

run().catch(console.error);
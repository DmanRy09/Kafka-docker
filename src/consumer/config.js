import dotenv from 'dotenv';
dotenv.config();

export const KAFKA_BROKER = process.env.KAFKA_BROKER || process.env.KAFKA_HOST_BROKER || 'localhost:29092';

// Use shared group if provided; otherwise create a per-user group id
const userId = process.env.CONSUMER_GROUP ||
               process.env.USER ||
               process.env.USERNAME ||
               process.env.COMPUTERNAME ||
               'local';
export const GROUP_ID = `app-consumer-group-${userId}`;

export const CONFIG = {
  kafka: {
    brokers: [KAFKA_BROKER],
    topics: {
      payments: 'payments',
      sales: 'sales'
    }
  }
};
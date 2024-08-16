import { Kafka, Message } from 'kafkajs';

const kafka = new Kafka({ brokers: ["localhost:9092"], clientId: "kafka-js-client-id" })


const producer = kafka.producer();

export const producerMessage = async (message: any, topic: string) => {
    await producer.connect();
    await producer.send({ messages: [{ value: JSON.stringify(message) }], topic });
    await producer.disconnect();
}





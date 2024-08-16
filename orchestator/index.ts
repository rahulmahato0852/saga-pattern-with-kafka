import { Kafka } from 'kafkajs';
import { ORDER_FAILED, ORDER_REQUEST, ORDER_SUCCESS, PAYMENT_FAILED, PAYMENT_REQUEST, PAYMENT_SUCCESS, STOOCK_FAILED, STOOCK_REQUEST, STOOCK_SUCCESS } from '../kafka/topic';
import { producerMessage } from '../kafka/kafka';


const kafka = new Kafka({ brokers: ["localhost:9092"], clientId: "orchestator-kafka-client-id" });

const consumer = kafka.consumer({ groupId: "orchestator-kafka-group-id" });

(async () => {

    await consumer.connect();

    await consumer.subscribe({ topics: [ORDER_REQUEST, STOOCK_FAILED, STOOCK_SUCCESS, PAYMENT_SUCCESS, PAYMENT_FAILED], fromBeginning: false });
    await consumer.run({
        eachMessage: async ({ message, topic }) => {
            const data = JSON.parse(message.value?.toString()!)
            console.log("TOPIC IN ORCHESTATOR", topic, "====><====", data);

            if (topic === ORDER_REQUEST) {
                await producerMessage(data, STOOCK_REQUEST)
            } else if (topic === STOOCK_SUCCESS) {
                console.log("Payment request producing");
                await producerMessage({ message: data }, PAYMENT_REQUEST)
                console.log("Payment request produced");
            } else if (topic === STOOCK_FAILED) {
                await producerMessage({ message: "NO STOCK AVAILABLE OF THIS PRODUCT ON THIS TIME" }, ORDER_FAILED)
            } else if (topic === PAYMENT_SUCCESS) {
                console.log("order success producing");
                await producerMessage({ message: data }, ORDER_SUCCESS);
                console.log("order success produced");
            } else if (topic === PAYMENT_FAILED) {
                console.log("order failed producing");
                await producerMessage({ message: data }, ORDER_FAILED)
                console.log("order failed produced");
            }
        }
    })
})()



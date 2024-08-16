import express, { Response, Request } from 'express';
import { Kafka } from 'kafkajs';
import { PAYMENT_FAILED, PAYMENT_REQUEST, PAYMENT_SUCCESS } from '../kafka/topic';
import { producerMessage } from '../kafka/kafka';


const app = express();

const kafka = new Kafka({ brokers: ["localhost:9092"], clientId: "payment-kafka-clientId" })
const consumer = kafka.consumer({ groupId: "kafka-consumer-groupId" });


app.use(express.json())






app.post("/", async (req: Request, res: Response) => {

    await consumer.connect();
    console.log("Consumer connected");
    await consumer.subscribe({ topics: [PAYMENT_REQUEST], fromBeginning: true });
    console.log("Consumer subscribed");
    await consumer.run({
        eachMessage: async ({ message, topic }) => {
            try {
                console.log("MESSAGE-------------------");
                const data = JSON.parse(message.value?.toString()!);
                console.log("Payment request received", data);
                if (req.body.status === "success") {
                    await producerMessage({ message: "Payment successfully", data }, PAYMENT_SUCCESS);
                    res.status(200).json({ message: "Payment successfully ♥♥ " });
                } else {
                    await producerMessage({ message: "Payment failed", data }, PAYMENT_FAILED);
                    res.status(400).json({ message: "Payment failed 😞😞 " });
                }
            } catch (error) {
                res.status(500).json({ message: "Payment failed by server", error })
            } finally {
                consumer.disconnect();
                console.log("Consumer disconnect");
            }
        }
    })
    console.log("consumer runned");


})




app.listen(8000, () => {
    console.log("PAYMENT SERVER RURNIING ON  8000");
})
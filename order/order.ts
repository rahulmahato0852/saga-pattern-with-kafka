import express, { Request, Response } from 'express'
import mongoose from 'mongoose'
import { producerMessage } from '../kafka/kafka';
import { ORDER_FAILED, ORDER_REQUEST, ORDER_SUCCESS } from '../kafka/topic';
import { Kafka } from 'kafkajs';

const app = express();

app.use(express.json())
app.use(express.urlencoded({ extended: true }))

mongoose.connect("mongodb://localhost:27017/saga")



const kafka = new Kafka({ brokers: ["localhost:9092"], clientId: "order-kafka-client-id" })
const consumer = kafka.consumer({ groupId: "order-kafka-group-id" });



app.post("/", async (req: Request, res: Response) => {

    await producerMessage(req.body, ORDER_REQUEST);

    await consumer.connect();
    await consumer.subscribe({ topics: [ORDER_SUCCESS, ORDER_FAILED], fromBeginning: false });
    await consumer.run({
        eachMessage: async ({ message, topic }) => {
            try {
                const data = JSON.parse(message.value?.toString()!)
                if (topic === ORDER_SUCCESS) {
                    res.status(200).json({ message: "order success", data })
                } else if (topic === ORDER_FAILED) {
                    res.status(400).json({ message: "order failed", data })
                } else {
                    res.status(400).json({ message: "No toipc matched", data })
                }
            } catch (error) {
                res.status(500).json({ message: "Failed", error })
            } finally {
                consumer.disconnect();
                console.log("Consumer disconnected");
            }
        }
    })

});


mongoose.connection.once("open", () => {
    console.log("MONGOOSE CONNECTED");
    app.listen(3000, () => {
        console.log("ORDER SERVER RUNNING ON 3000")
    })
})
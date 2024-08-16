import express, { Request, Response } from "express";
import mongoose from "mongoose";
import { Kafka } from 'kafkajs'
import { STOOCK_FAILED, STOOCK_REQUEST, STOOCK_SUCCESS } from "../kafka/topic";
import { producerMessage } from "../kafka/kafka";

const app = express();
const kafka = new Kafka({ brokers: ["localhost:9092"], clientId: "stock-kafka-clientId" });
app.use(express.json());
const Products = mongoose.model("products", new mongoose.Schema({
    name: { type: String, required: true },
    company: { type: String, required: true },
    price: { type: Number, required: true },
    stock: { type: Number, required: true },
}, { timestamps: true }));
mongoose.connect("mongodb://localhost:27017/saga");


app.post("/add-product", async (req: Request, res: Response) => {
    const result = await Products.create(req.body);
    res.status(201).json({ message: "Product added success", result })
})



mongoose.connection.once("open", () => {
    console.log("MONGOOSE CONNECTED");
    app.listen(4000, () => {
        console.log("STOCK SERVER RUNNING ON 4000");
    })

});












































(async () => {
    const consumer = kafka.consumer({ groupId: "stock-consumer-groupId" });
    await consumer.connect();
    await consumer.subscribe({ topic: STOOCK_REQUEST, fromBeginning: false });
    await consumer.run({
        eachMessage: async ({ message, topic }) => {
            try {
                const data = JSON.parse(message.value?.toString()!);
                console.log("data", data);
                const productId = data.productId;;
                console.log(productId, "idddd");

                const result = await Products.findById(productId);
                if (result && result.stock >= +data.stock) {
                    await producerMessage({ data, result }, STOOCK_SUCCESS);
                } else {
                    await producerMessage({ data, result }, STOOCK_FAILED);
                }
            } catch (error) {
                await producerMessage({ error: "Unable to check stock" }, STOOCK_FAILED);

            }
        }
    })
})();


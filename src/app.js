require('dotenv').config();
import express from 'express';
import { Producer } from 'node-rdkafka';

const PORT = process.env.PORT || 3000
const app = express()

const producer = new Producer({
    'bootstrap.servers': process.env.KAFKA_BOOTSTRAP_SERVERS,
    'sasl.username': process.env.KAFKA_SASL_USERNAME,
    'sasl.password': process.env.KAFKA_SASL_PASSWORD,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    // Ensure only-once delivery semantics
    'enable.idempotence' : true,
    // Enable to receive delivery reports for messages
    'dr_cb': true,
    // Enable to receive message payload in delivery reports
    'dr_msg_cb': true,
  });
//logging debug messages, if debug is enabled
producer.on('event.log', function(log) {
    console.log(log);
});
producer.on('disconnected', function(arg) {
    console.log('producer disconnected. ' + JSON.stringify(arg));
});
producer.on('ready', () => {
    console.log('Kafka Producer ready')
})
producer.on('event.error', (err) => {
    console.warn('event.error', err);
    });    
producer.on('delivery-report', (err, report) => {
    console.log('delivery-report', ',error =',err, ',report =', JSON.stringify(report));
    });
producer.setPollInterval(100);
producer.connect();

app.get('/status', (req, res) => {
    res.send(`Service OK`)
    producer.produce(process.env.KAFKA_TOPIC, 
        null, 
        Buffer.from(JSON.stringify("test")))
})


app.listen(PORT, () => {
    console.log(process.env.KAFKA_BOOTSTRAP_SERVERS)
    console.log(process.env.KAFKA_TOPIC)
    console.log('KAFKA_SASL_USERNAME: ', process.env.KAFKA_SASL_USERNAME)
    console.log('Server is running on port ', PORT)
})

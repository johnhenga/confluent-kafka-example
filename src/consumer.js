const Kafka = require('node-rdkafka');
// const uuid = require('uuid/v4');
require('dotenv').config();

function createConsumer(config, onData) {
    const consumer = new Kafka.KafkaConsumer({
      'bootstrap.servers': process.env.KAFKA_BOOTSTRAP_SERVERS,
      'sasl.username': process.env.KAFKA_SASL_USERNAME,
      'sasl.password': process.env.KAFKA_SASL_PASSWORD,
      'security.protocol': process.env.KAFKA_SECURITY_PROTOCOL,
      'sasl.mechanisms': process.env.KAFKA_SASL_MECHANISM,
    //   'client.id': `consumer-${uuid()}`,
      'group.id': process.env.KAFKA_GROUP_ID 
    }, {
      'auto.offset.reset': 'latest'
    });
    
    return new Promise((resolve, reject) => {
      consumer
      .on('ready', () => resolve(consumer))
      .on('data', onData);
      consumer.connect();
    });
  }

async function pointProcessor() {
    const config = {
        'topic': process.env.KAFKA_TOPIC,
    };    
    console.log(`Consuming records from Topic : ${config.topic}`);
    let seen = 0;
    const consumer = await createConsumer(config, ({key, value, partition, offset}) => {
      try {
        // var event_details = JSON.parse(value);
        console.log(value)
      } catch (error) {
        console.log(error);
      }
    });

    consumer.subscribe([config.topic]);
    consumer.consume();

    process.on('SIGINT', () => {
    console.log('\nDisconnecting consumer ...');
    consumer.disconnect();
    });
}

if (require.main === module) {
  pointProcessor()
    .catch((err) => {
      console.error(`Something went wrong:\n${err}`);
      process.exit(1);
    });
}

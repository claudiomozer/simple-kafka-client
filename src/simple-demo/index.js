import { Kafka } from 'kafkajs'

const kafkaClient = new Kafka({
  clientId: 'simple-producer-consumer-application',
  brokers: ['localhost:9092']
});

const runProducer = async () => {
  const kafkaProducer = kafkaClient.producer();
  await kafkaProducer.connect();

  await kafkaProducer.send({
    topic: 'simple-topic',
    messages: [
      { value: 'simple-test' }
    ]
  });

  await kafkaProducer.disconnect();
}

const startConsumer = async () => {
  const kafkaConsumer = kafkaClient.consumer({ groupId: 'simple-groups' })

  await kafkaConsumer.connect();
  await kafkaConsumer.subscribe({ topic: 'simple-topic', fromBeginning: true });
  
  await kafkaConsumer.run({

    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      })
    }
  })
}

runProducer().then(() => {
  startConsumer();
});

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
    acks: 0, // 0 - sem acknowledgement, 1 - aguarda o leader responder, (all | -1) - aguarda as resposetas dos leaders e ISRs
    messages: [
      {
        key: 'key',
        value: 'simple-test',
        partition: 0 // todas as mensagens com essa chave vão para a partição 0
      },
      {
        key: 'key2',
        value: 'simple-test2',
        headers: {
          'correlation-id': 'some-uuid'
        }
      },
      {
        key: 'key3',
        value: 'simple-test3'
      }
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
        key: message.key.toString(),
        value: message.value.toString(),
        headers: message.headers,
        topic,
        partition
      })
    }
  })
}

runProducer().then(() => {
  startConsumer();
});

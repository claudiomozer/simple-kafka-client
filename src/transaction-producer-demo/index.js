import { Kafka } from 'kafkajs'

const kafkaClient = new Kafka({
  clientId: 'transaction-consumer-application',
  brokers: ['localhost:9092']
});

const runProducer = async () => {
  const kafkaProducer = kafkaClient.producer({
    idempotent: true,
    maxInFlightRequests: 1,
    transactionalId: 'some-potato-id'
  });
  await kafkaProducer.connect();
  const transaction = await kafkaProducer.transaction();

  try {
    await transaction.send({
      topic: 'quickstart',
      messages: [
        {
          key: 'transaction1',
          value: 'simple-test2'
        },
        {
          key: 'transaction2',
          value: 'simple-test3'
        }
      ]
    });

    // throw new Error('damn'); - Podemos tentar jogar uma excessão, neste caso as mensagens não serão enviadas
    await transaction.commit();
  } catch (error) {
    console.log(`An error ocurred: ${error.message}`)
    await transaction.abort();
  } finally {
    await kafkaProducer.disconnect();
  }
}

const startConsumer = async () => {
  const kafkaConsumer = kafkaClient.consumer({ groupId: 'asdgroups' })

  await kafkaConsumer.connect();
  await kafkaConsumer.subscribe({ topic: 'quickstart', fromBeginning: true });
  
  await kafkaConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        key: message.key?.toString() ?? 'no key provided',
        value: message.value?.toString() ?? 'no value provided',
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
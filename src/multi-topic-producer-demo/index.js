import { Kafka } from 'kafkajs'

const kafkaClient = new Kafka({
  clientId: 'simple-producer-consumer-application',
  brokers: ['localhost:9092']
});

const runProducer = async () => {
  const kafkaProducer = kafkaClient.producer();
  await kafkaProducer.connect();

  const topicMessages = [
    {
      topic: 'simple-topic',
      acks: -1, // 0 - sem acknowledgement, 1 - aguarda o leader responder, (all | -1) - aguarda as resposetas dos leaders e ISRs
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
    },
    {
      topic: 'quickstart',
      acks: 0, // 0 - sem acknowledgement, 1 - aguarda o leader responder, (all | -1) - aguarda as resposetas dos leaders e ISRs
      messages: [
        {
          key: '1',
          value: 'simple-test',
        },
        {
          key: 'key3',
          value: 'simple-test3'
        }
      ]
    }
  ];

  await kafkaProducer.sendBatch({ topicMessages });

  await kafkaProducer.disconnect();
}

const processMessage = async ({
  batch,
  resolveOffset,
  heartbeat,
  commitOffsetsIfNecessary,
  uncommitedOffsets,
  isRunning,
  isState
}) => {
  for(const message of batch.messages) {
    console.log({
      topic: batch.topic,
      message: {
        offset: message.offset ?? 'no offset',
        key: message.key?.toString() ?? 'no key provided',
        value: message.value?.toString() ?? 'no value provided',
      }
    });
    resolveOffset(message.offset); // Infoma o kafka para incrementar o offset para essa mensagem, informando que já foi lida
    await heartbeat(); // 
  }
}

const startConsumer = async () => {
  const kafkaConsumer = kafkaClient.consumer({ groupId: 'simple1' })

  await kafkaConsumer.connect();
  await kafkaConsumer.subscribe({ topic: 'simple-topic', fromBeginning: true });
  await kafkaConsumer.subscribe({ topic: 'quickstart', fromBeginning: true });
  
  await kafkaConsumer.run({
    eachBatchAutoResolve: true,
    eachBatch: processMessage
  })
}


runProducer().then(() => {
  startConsumer();
});

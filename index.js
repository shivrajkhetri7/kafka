const { Kafka } = require("kafkajs");

const init = async () => {
  const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["kafka1:9092", "kafka2:9092"],
  });

  /**
   *  ! Topic Implementation
   */
  const producer = kafka.producer();

  await producer.connect();
  await producer.send({
    topic: "test-topic",
    messages: [{ value: "Hello KafkaJS user!" }],
  });

  await producer.disconnect();

  /**
   *  ! Consumers
   */
  const consumer = kafka.consumer({ groupId: "test-group" });

  await consumer.connect();
  await consumer.subscribe({ topic: "test-topic", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      });
    },
  });
};

init();

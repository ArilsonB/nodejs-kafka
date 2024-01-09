const express = require('express');
const bodyParser = require('body-parser');
const { Kafka } = require('kafkajs');

const app = express();
const port = 3000;

app.use(bodyParser.json());

// Kafka producer configuration
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
});

async function listTopics() {
  const admin = kafka.admin();
  try {
    await admin.connect()

    const metadata = await admin.fetchTopicMetadata();

    const topics = metadata.topics.map(topic => topic.name);

    console.log('List of topics:', topics);
    
  } catch(error) {
    console.error('Error fetching topic metadata:', error);
  } finally {
    await admin.disconnect();
  }


}

listTopics();

const producer = kafka.producer();

// Express endpoint to produce messages to Kafka
app.post('/produce', async (req, res) => {
  try {
    const { message } = req.body;

    // Produce message to Kafka
    await producer.connect();
    await producer.send({
      topic: 'my-topic',
      messages: [{ value: message }],
    });

    res.status(200).send('Message sent to Kafka');
  } catch (error) {
    console.error(error);
    res.status(500).send('Internal Server Error');
  }
});

// Kafka consumer configuration
const consumer = kafka.consumer({ groupId: 'my-group' });

// Express endpoint to consume messages from Kafka
app.get('/consume', async (req, res) => {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'my-topic', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log({
          value: message.value.toString(),
        });
      },
    });

    res.status(200).send('Consuming messages from Kafka');
  } catch (error) {
    console.error(error);
    res.status(500).send('Internal Server Error');
  }
});

app.listen(port, () => {
  console.log(`Server is running at http://localhost:${port}`);
});

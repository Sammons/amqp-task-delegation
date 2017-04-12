require('source-map-support').install();
import { DelegatedConsumer as Consumer, DelegatedPublisher as Publisher } from 'amqp-task-delegation';
import * as assert from 'assert';
import * as mocha from 'mocha';
import * as amqp from 'amqp-ts';
import * as uuid from 'uuid';

const TestRabbitConnectionStr = process.env['rabbit_uri'] || 'amqp://guest:guest@localhost:5672/%2F';

const GetTestConnection = async () => {
  let connection = new amqp.Connection(TestRabbitConnectionStr);
  await connection.initialized;
  return connection;
}

const DeclareExchange = async (name, type: 'fanout', options: amqp.Exchange.DeclarationOptions, c?: amqp.Connection) => {
  let connection = c || await GetTestConnection();
  let ex = new amqp.Exchange(connection, name, type, options);
  await ex.initialized;
  return ex;
}

const DeclareQueue = async (name, options: amqp.Queue.DeclarationOptions, c?: amqp.Connection) => {
  let connection = c || await GetTestConnection();
  let queue = new amqp.Queue(connection, name, options);
  await queue.initialized;
  return queue;
}

const SingleShardPublisherExtractor = () => '1';
const CreateSingleShardPublisherWithNoDepthHandler = async (namespace: string, c?: amqp.Connection) => {
  let connection = c || await GetTestConnection();
  let publisher = new Publisher({
    connection,
    distributionQueueDepthHandler: null,
    namespace,
    shardExtractor: SingleShardPublisherExtractor,
    options: { durable: true }
  });
  await publisher.initialized;
  return publisher;
}

const CleanupSingleShardPublisherTest = async (publisher: Publisher<any>) => {
  let consumer = await new Consumer({
    namespace: publisher.namespace,
    connection: publisher.connection
  });
  consumer.consumeTasks((msg) => { msg.ack(false) });
  await consumer.stopConsumingTasks();
  await publisher.deleteTaskDistributionQueue();
}

describe('Environment', function () {
  describe('connection', function () {
    it('should be able to connect to the test rabbit', async function () {
      let connection = await GetTestConnection();
      await connection.close();
    });
    it('should be able to declare and delete a queue', async function () {
      const testQueueName = uuid.v4();
      let queue = await DeclareQueue(testQueueName, {});
      await queue.delete();
      try {
        queue = await DeclareQueue(testQueueName, { noCreate: true });
      } catch (e) {
        queue = null;
      }
      assert(queue == null, 'Queue should not exist');
    });
  });
});

describe('AMQP Task Delegation', function () {
  describe('publisher', function () {

    it('should create a task queue upon publish', async function () {
      let namespace = uuid.v4();
      let publisher = await CreateSingleShardPublisherWithNoDepthHandler(namespace);
      await publisher.publish([{ test: 'value' }], {});
      let taskQueueName = publisher.getTaskQueueName(SingleShardPublisherExtractor());
      let queue: amqp.Queue;
      try {
        queue = await DeclareQueue(taskQueueName, {
          noCreate: true
        });
      } catch (e) {
        queue = null;
      }
      assert(queue != null, 'Queue should exist');
      await CleanupSingleShardPublisherTest(publisher)
    });

    it('should delete task queue')
  })
})
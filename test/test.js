"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
require('source-map-support').install();
const amqp_task_delegation_1 = require("amqp-task-delegation");
const assert = require("assert");
const amqp = require("amqp-ts");
const uuid = require("uuid");
const TestRabbitConnectionStr = process.env['rabbit_uri'] || 'amqp://guest:guest@localhost:5672/%2F';
const GetTestConnection = () => __awaiter(this, void 0, void 0, function* () {
    let connection = new amqp.Connection(TestRabbitConnectionStr);
    yield connection.initialized;
    return connection;
});
const DeclareExchange = (name, type, options, c) => __awaiter(this, void 0, void 0, function* () {
    let connection = c || (yield GetTestConnection());
    let ex = new amqp.Exchange(connection, name, type, options);
    yield ex.initialized;
    return ex;
});
const DeclareQueue = (name, options, c) => __awaiter(this, void 0, void 0, function* () {
    let connection = c || (yield GetTestConnection());
    let queue = new amqp.Queue(connection, name, options);
    yield queue.initialized;
    return queue;
});
const SingleShardPublisherExtractor = () => '1';
const CreateSingleShardPublisherWithNoDepthHandler = (namespace, c) => __awaiter(this, void 0, void 0, function* () {
    let connection = c || (yield GetTestConnection());
    let publisher = new amqp_task_delegation_1.DelegatedPublisher({
        connection,
        distributionQueueDepthHandler: null,
        namespace,
        shardExtractor: SingleShardPublisherExtractor,
        options: { durable: true }
    });
    yield publisher.initialized;
    return publisher;
});
const CleanupSingleShardPublisherTest = (publisher) => __awaiter(this, void 0, void 0, function* () {
    let consumer = yield new amqp_task_delegation_1.DelegatedConsumer({
        namespace: publisher.namespace,
        connection: publisher.connection
    });
    consumer.consumeTasks((msg) => { msg.ack(false); });
    yield consumer.stopConsumingTasks();
    yield publisher.deleteTaskDistributionQueue();
});
describe('Environment', function () {
    describe('connection', function () {
        it('should be able to connect to the test rabbit', function () {
            return __awaiter(this, void 0, void 0, function* () {
                let connection = yield GetTestConnection();
                yield connection.close();
            });
        });
        it('should be able to declare and delete a queue', function () {
            return __awaiter(this, void 0, void 0, function* () {
                const testQueueName = uuid.v4();
                let queue = yield DeclareQueue(testQueueName, {});
                yield queue.delete();
                try {
                    queue = yield DeclareQueue(testQueueName, { noCreate: true });
                }
                catch (e) {
                    queue = null;
                }
                assert(queue == null, 'Queue should not exist');
            });
        });
    });
});
describe('AMQP Task Delegation', function () {
    describe('publisher', function () {
        it('should create a task queue upon publish', function () {
            return __awaiter(this, void 0, void 0, function* () {
                let namespace = uuid.v4();
                let publisher = yield CreateSingleShardPublisherWithNoDepthHandler(namespace);
                yield publisher.publish([{ test: 'value' }], {});
                let taskQueueName = publisher.getTaskQueueName(SingleShardPublisherExtractor());
                let queue;
                try {
                    queue = yield DeclareQueue(taskQueueName, {
                        noCreate: true
                    });
                }
                catch (e) {
                    queue = null;
                }
                assert(queue != null, 'Queue should exist');
                yield CleanupSingleShardPublisherTest(publisher);
            });
        });
        it('should delete task queue');
    });
});

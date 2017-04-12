import * as amqp from 'amqp-ts';
import * as uuid from 'uuid';
// import * as bluebird from 'bluebird';
const debugDeclarationsOn = process.env['amqp_task_eventing_debug_declarations'] == 'true';

const debugDeclarations = (msg, other) => {
  if (debugDeclarationsOn) {
    console.log(new Date().toISOString(), msg || '', JSON.stringify(other || {}, null, 2));
  }
}

export class DelegatedPublisher<T> {

  public get connection() { return this.opts.connection; }
  public get namespace() { return this.opts.namespace; }

  constructor(private opts: {
    namespace: string;
    shardExtractor: (t: T) => string | Promise<string>;
    connection: amqp.Connection;
    distributionQueueDepthHandler: (depth: number) => void | Promise<void>;
    options: {
      durable?: boolean;
    };
  }) {
    if (!this.opts.options) {
      this.opts.options = {};
    }
    if (!this.opts.options.durable) {
      this.opts.options.durable = true;
    }
    debugDeclarations('task shard publisher initialized', { namespace: this.opts.namespace });
    this._initialized = this.initialize();
  }
  private publisherId = uuid.v4();
  private _initialized: Promise<void> = null;
  public get initialized(): Promise<void> { return this._initialized; }

  public getSupervisionExchangeName() {
    return 'supervise-' + this.opts.namespace
  }

  public getSupervisionQueueName() {
    return 'supervise-' + this.opts.namespace + '-' + this.publisherId
  }

  private async getSupervisionExchange() {
    let exchange = new amqp.Exchange(this.opts.connection, this.getSupervisionExchangeName(), 'fanout');
    await exchange.initialized;
    return exchange;
  }

  private async getSupervisionQueue() {
    let queue = new amqp.Queue(
      this.opts.connection,
      this.getSupervisionQueueName(), {
        durable: this.opts.options.durable
      });
    await queue.initialized;
    return queue;
  }

  private async initialize() {
    await this.opts.connection.initialized;

    let mySupervisionQueue = await this.getSupervisionQueue();
    let init = await mySupervisionQueue.initialized;

    /** activateConsumer logs wierd error, so still using deprecated version */
    await mySupervisionQueue.startConsumer(async (msg) => {
      try {
        if (msg) {
          let content: { command: 'shutdown' } = JSON.parse(msg.getContent());
          if (content.command === 'shutdown') {
            this.shuttingDown = true;
            await this.freezePublishingAndUnhookSupervision();
          }
        }
        msg.ack()
      } catch (e) {
        console.log('Error internally consuming command from supervision queue');
        throw e;
      }
    }, { consumerTag: 'publisher-' + this.publisherId });


    if (init.consumerCount > 0) {
      throw new Error(`This publisher already existed. that's not supposed to happen.`);
    }
    let exchange = await this.getSupervisionExchange();
    let binding = await mySupervisionQueue.bind(exchange, '#');
    await binding.initialized
  }

  public getTaskQueueName(shard: string): string {
    let shardQueueName = 'task-queue-' + (this.opts.namespace || 'default') + '-shard-' + shard;
    debugDeclarations('shard queue name generated', { shardQueueName })
    return shardQueueName
  }
  private async getTaskQueue(shard: string): Promise<amqp.Queue> {
    debugDeclarations('generating shard queue', { shard });

    await this._initialized;

    let queue = this.opts.connection.declareQueue(this.getTaskQueueName(shard), {
      autoDelete: true,
      durable: this.opts.options.durable
    });
    let initResults = await queue.initialized;
    debugDeclarations('generated shard queue', { shard, initResults });
    return queue;
  }

  public getTaskDistributionQueueName(): string {
    return 'task-queue-' + this.opts.namespace;
  }

  private async getTaskDistributionQueue() {
    await this._initialized;

    let distributionQueueOpts = {
      durable: this.opts.options.durable,
      prefetch: 1
    };

    debugDeclarations('declaring primary distribution queue', { distributionQueueOpts });
    let distributionQueue = new amqp.Queue(this.opts.connection, this.getTaskDistributionQueueName(), distributionQueueOpts);
    let distributionQueueInitialization = await distributionQueue.initialized;

    debugDeclarations('declared primary distribution queue', { distributionQueueInitialization });
    if (this.opts.distributionQueueDepthHandler) {

      debugDeclarations('applying distribution depth handler', {})
      await this.opts.distributionQueueDepthHandler(distributionQueueInitialization.messageCount);
      debugDeclarations('applied successfully', {})
    }

    return distributionQueue;
  }

  private publishing: Promise<void> = Promise.resolve();
  private shuttingDown: boolean;
  public async publish<V extends object>(messages: (Promise<(V | string)> | (V | string))[], shardBy: T): Promise<void> {
    if (this.shuttingDown) {
      throw new Error(`Cannot publish after shutdown started`);
    }
    await this.publishing;
    this.publishing = (async () => {
      debugDeclarations('publishing ' + messages.length + ' messages', { shardBy });
      await this._initialized;
      debugDeclarations('connection ready, publishing', {})
      let shard = await this.opts.shardExtractor(shardBy);
      let distributionQueue = this.getTaskDistributionQueue();
      let taskQueue = this.getTaskQueue(shard);

      messages = (await Promise.all(messages)).map(msg => typeof msg === 'object' ? JSON.stringify(msg) : msg.toString());

      return taskQueue.then(async (queue) => {
        let distribution = await distributionQueue;
        await Promise.all(messages.map(
          msg => {
            return queue.send(new amqp.Message(msg))
          }));
        await queue.close();
        distribution.send(new amqp.Message({
          taskQueueName: queue.name
        }))
      })
    })();
    return this.publishing;
  }

  public async freezePublishingAndUnhookSupervision(): Promise<void> {
    this.shuttingDown = true;
    let supervisionQueue = await this.getSupervisionQueue();
    let del = await supervisionQueue.delete();
    debugDeclarations('Deleted supervision queue', { del });
  }

  public async deleteTaskDistributionQueue(): Promise<void> {
    let q = await this.getTaskDistributionQueue();
    await q.delete();
  }
}
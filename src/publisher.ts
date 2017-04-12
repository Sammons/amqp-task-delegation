import * as amqp from 'amqp-ts';
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
  private _initialized: Promise<void> = null;
  public get initialized(): Promise<void> { return this._initialized; }


  private async initialize() {
    await this.opts.connection.initialized;
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

  public async deleteTaskDistributionQueue(): Promise<void> {
    let q = await this.getTaskDistributionQueue();
    await q.delete();
  }
}
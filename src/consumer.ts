import * as amqp from 'amqp-ts';
// import * as bluebird from 'bluebird';
// import * as fs from 'fs';
const debugDeclarationsOn = process.env['amqp_task_eventing_debug_declarations'] == 'true';

const debugDeclarations = (msg, other) => {
  if (debugDeclarationsOn) {
    console.log(new Date().toISOString(), msg || '', JSON.stringify(other || {}, null, 2));
  }
}

export class DelegatedConsumer {
  constructor(private opts: {
    namespace: string;
    connection: amqp.Connection;
  }) {
    debugDeclarations('task shard consumer initialized', { namespace: opts.namespace });
  }

  private async getTaskQueue(name: string) {
    debugDeclarations('declaring shard queue', { name });

    await this.opts.connection.initialized;

    let queue = new amqp.Queue(this.opts.connection, name, {
      autoDelete: true,
      durable: true,
      noCreate: true
    });
    let initResults = await queue.initialized;
    debugDeclarations('declared shard queue', { name, initResults });
    return { queue, initResults };
  }

  private getTaskDistributionQueueName(): string {
    return 'task-queue-' + this.opts.namespace;
  }

  private async getTaskDistributionQueue() {
    await this.opts.connection.initialized;

    let distributionQueueOpts = {
      durable: true,
      prefetch: 1
    };

    debugDeclarations('declaring primary distribution queue', { distributionQueueOpts });
    let distributionQueue = this.opts.connection.declareQueue(this.getTaskDistributionQueueName(), distributionQueueOpts);
    let distributionQueueInitialization = await distributionQueue.initialized;

    debugDeclarations('declared primary distribution queue', { distributionQueueInitialization });

    return distributionQueue;
  }

  private async acquireNextFreeTaskQueue() {
    return new Promise<amqp.Queue>(async (resolve, reject) => {
      try {
        await this.opts.connection.initialized;

        let distributionQueue = await this.getTaskDistributionQueue();

        let shutdownInterval = setInterval(async () => {
          debugDeclarations('Considering canceling distribution consumption', { shouldContinue: this.continueConsuming });
          if (!this.continueConsuming) {
            try {
              await distributionQueue.stopConsumer();
            } catch (e) {
              debugDeclarations(`Failed to stop consumer`, { message: e.message });
            }
            clearInterval(shutdownInterval);
            resolve(null);
          }
        }, 500);

        let activationResult = await distributionQueue.activateConsumer(async (msg) => {
          try {
            let task: {
              taskQueueName: string
            } = msg.getContent();
            let { initResults, queue } = await this.getTaskQueue(task.taskQueueName);

            if (initResults.consumerCount == 0) {
              debugDeclarations('task acquired, stopped taking new assignments', { task });
              await distributionQueue.stopConsumer();
              resolve(queue);
              msg.ack(false);
            } else if (initResults.messageCount > 0 && initResults.consumerCount > 0) {
              debugDeclarations('Requeing task, not usable', {});
              msg.nack(false)
            } else {
              debugDeclarations('proceeding with task observations', {});
              msg.ack(false);
            }
          } catch (e) {
            console.log('error', e.message)
            msg.ack(false)
          }
        });

        debugDeclarations('distribution consumption activated', { activationResult });
      } catch (e) {
        reject(e);
      }
    })
  }

  private async queueIsEmpty(queue: amqp.Queue, intervalMs: number): Promise<void> {
    return new Promise<void>(async (resolve, reject) => {
      try {
        let interval = setInterval(async () => {
          try {
            debugDeclarations('testing queue for emptiness', { name: queue.name });
            let q = await new amqp.Queue(this.opts.connection, queue.name, {
              autoDelete: true,
              durable: true
            });
            let initResult = await q.initialized;
            debugDeclarations('queue emptiness', { initResult });
            if (initResult.messageCount === 0) {
              clearInterval(interval);
              resolve();
            }
          } catch (e) {
            clearInterval(interval);
            reject(e);
          }
        }, intervalMs);
      } catch (e) {
        reject(e);
      }
    })
  }

  private continueConsuming = false;
  private taskConsumptionWatcher: Promise<void> = Promise.resolve();

  async stopConsumingTasks(): Promise<void> {
    this.continueConsuming = false;
    return this.taskConsumptionWatcher;
  }

  messageHandler: (msg: amqp.Message) => Promise<void> | void;

  private async consume(messageHandler: this['messageHandler']): Promise<void> {
    while (this.continueConsuming) {
      let queue = await this.acquireNextFreeTaskQueue();
      if (queue) {
        let activated = await queue.activateConsumer(messageHandler);
        debugDeclarations('activated consumption of tasks from queue', { queuename: queue.name, activated });
        await this.queueIsEmpty(queue, 300/** interval */);
        await queue.stopConsumer();
        await queue.close();
        debugDeclarations('finished consumption of tasks from queue', { queuename: queue.name });
      } else {
        debugDeclarations('no queue returned by queue acquisition, we are probably in shut down', {});
      }
    }
  }

  consumeTasks(messageHandler: this['messageHandler']) {
    if (!this.continueConsuming) {
      this.messageHandler = messageHandler;
      this.continueConsuming = true;
      this.taskConsumptionWatcher = this.consume(messageHandler);
    } else {
      if (messageHandler !== this.messageHandler) {
        throw new Error(`Already consuming tasks with a different handler`);
      }
    }
  }

}
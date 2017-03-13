The point of this repository *task-eventing* is to facilitate a situation where:

* the publisher wants their messages to be processed in sequence by the same consumer
* the publisher wants to segregate their messages property
* the publisher wants to utilize an arbitrary, dynamic, number of consumers
* the publisher does not want to worry about the number of shards it is creating

The method applied so far is as follows:

There is a single distribution queue, which all consumers bind to. Each message in the queue indicates that a batch of messages has previously been published to a particular task queue, and that they await consumption. Upon consuming a distribution message, the consumer inspects the noted task queue. If the queue is already claimed (has a consumer), but is not yet empty, then requeue the distribution message in case the other consumer fails to properly drain the task queue. If the queue is claimed and is empty, then ack the distribution message upon completing drainage of the queue. If the queue is neither claimed nor empty, then claim it and drain it, subsequently acking the distribution message.

With this method, there are lots of distribution messages which ultimately result in no action, however it allows us to delegate an unknown number of shards to an unknown number of consumers, and safely know that only a single consumer will process one of those shards at once.

This applies to AMQP 0.9.1, and the implementation is using RabbitMQ with node.js
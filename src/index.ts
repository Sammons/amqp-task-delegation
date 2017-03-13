if (!process.env.AMQPTS_APPLICATIONNAME) {
  /** shuts up internal amqp-ts when using in repl */
  process.env.AMQPTS_APPLICATIONNAME = 'amqp-task-delegation';
}

export * from './publisher';
export * from './consumer';
// main.js
const SmartScheduler = require('./smartScheduler');

const scheduler = new SmartScheduler();
scheduler.start();

// 保持进程运行
process.on('SIGINT', () => {
  logger.info('关闭调度器...');
  process.exit(0);
});
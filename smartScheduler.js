// smartScheduler.js
const logger = require('./logger');
const { spawn } = require('child_process');

// 直接在这里定义脚本配置（从 scriptRunner.js 复制过来）
const scripts = [
  {
    name: '更新Quote',
    command: 'node',
    args: ['svcUpdateQuote.js', '--immediate'],
    logFile: 'svcCalling.log',
    continueOnError: true // 即使失败也继续，使用旧价格
  },
  {
    name: '更新ExchangeRate',
    command: 'node',
    args: ['svcUpdateExchangeRate.js', '--immediate'],
    delay: 1 * 60 * 1000, // 缩短等待时间
    logFile: 'svcCalling.log',
    continueOnError: true // 即使失败也继续，使用旧汇率
  },
  {
    name: '汇总计算Holding数据',
    command: 'node',
    args: ['svcHoldingAggregationTask.js', '--immediate'],
    delay: 1 * 60 * 1000,
    logFile: 'svcCalling.log',
    continueOnError: false // 核心计算失败则后续导出无意义
  },
  {
    name: '资产负债表更新',
    command: 'node',
    args: ['svcPeriodicalBalanceSheetAll.js', '--immediate'],
    delay: 1 * 60 * 1000,
    logFile: 'svcCalling.log',
    continueOnError: true // 允许失败
  },
  {
    name: 'tblHoldingAggrView导出到Firebase',
    command: 'node',
    args: ['toolSmith/toolDuckDB2Firebase.js', 'tblHoldingAggrView', 'reports/holdings', '--no-key-check'], // 修正路径
    delay: 1 * 60 * 1000,
    logFile: 'firebase-export.log',
    continueOnError: true
  },
  {
    name: '资产负债表导出到Firebase',
    command: 'node',
    args: ['toolSmith/toolDuckDB2Firebase.js', 'tblPeriodicBalanceSheet', 'reports/balanceSheet'], // 修正路径
    logFile: 'firebase-export.log',
    continueOnError: true
  }
];

class SmartScheduler {
  constructor() {
    this.lastRun = null;
    this.isRunning = false;
    this.dbLocker = this.initializeDBLocker(); // 初始化数据库锁
    this.immediateMode = process.argv.includes('--immediate'); // 检查是否立即执行模式
  }

  initializeDBLocker() {
    try {
      return require('./dbLocker');
    } catch (error) {
      logger.warn('⚠️  dbLocker 模块未找到，使用内存锁替代');

      // 简单的内存锁实现
      return {
        locks: new Map(),
        async acquireLock(lockName) {
          if (this.locks.has(lockName)) {
            logger.warn(`⚠️ 锁 "${lockName}" 已被占用，跳过执行`);
            return false;
          }
          this.locks.set(lockName, true);
          logger.info(`🔒 获取内存锁: ${lockName}`);
          return true;
        },
        releaseLock(lockName) {
          this.locks.delete(lockName);
          logger.info(`🔓 释放内存锁: ${lockName}`);
        }
      };
    }
  }

  shouldRun() {
    // 如果是立即执行模式，直接返回 true
    if (this.immediateMode) {
      logger.info('🔴 立即执行模式激活，强制执行所有任务');
      return true;
    }

    const now = new Date();

    // 如果是第一次运行
    if (!this.lastRun) {
      logger.info('首次运行调度器');
      return true;
    }

    // 检查是否错过了今天的8:00执行
    const today8AM = new Date();
    today8AM.setHours(8, 0, 0, 0);

    const lastRunDate = new Date(this.lastRun);
    const shouldRunToday = now >= today8AM && lastRunDate < today8AM;

    if (shouldRunToday) {
      logger.info(`🔍 检测到错过执行，上次运行: ${lastRunDate.toLocaleString('zh-CN')}, 现在: ${now.toLocaleString('zh-CN')}`);
    }

    return shouldRunToday;
  }

  async runScripts() {
    if (this.isRunning) {
      logger.warn('⚠️ 任务正在执行中，跳过本次触发');
      return;
    }

    if (!this.shouldRun()) {
      if (this.immediateMode) {
        logger.info('🔄 立即执行模式：准备开始执行任务序列');
      } else {
        return;
      }
    }

    this.isRunning = true;
    const runStartTime = new Date();

    logger.info('='.repeat(60));
    if (this.immediateMode) {
      logger.info(`🚀 立即执行任务序列 - ${runStartTime.toLocaleString('zh-CN')}`);
    } else {
      logger.info(`🚀 开始执行每日任务序列 - ${runStartTime.toLocaleString('zh-CN')}`);
    }
    logger.info(`📝 任务数量: ${scripts.length}`);
    logger.info('='.repeat(60));

    try {
      for (let i = 0; i < scripts.length; i++) {
        const script = scripts[i];

        // 获取数据库锁（如果可用）
        const lockAcquired = await this.dbLocker.acquireLock(script.name, 15 * 60 * 1000);
        if (!lockAcquired) {
          logger.warn(`⏭️ 跳过任务: ${script.name} (锁被占用)`);
          continue;
        }

        try {
          await this.executeScript(script);
          logger.info(`✅ ${script.name} 完成`);

        } catch (scriptError) {
          if (script.continueOnError) {
            logger.error(`❌ ${script.name} 失败，但配置为继续执行: ${scriptError.message}`);
          } else {
            logger.error(`⛔ ${script.name} 失败，且为核心任务，终止序列`);
            throw scriptError; // 核心任务失败，中断整个流程
          }
        } finally {
          // 释放数据库锁
          this.dbLocker.releaseLock(script.name);

          // 如果不是最后一个脚本，且设置了延迟，则等待 (不管成功失败，只要继续执行就需要等待)
          // 只有在当前脚本没有抛出导致中断的错误时才会执行到这里
          if (i < scripts.length - 1 && scripts[i + 1].delay) {
            const nextScript = scripts[i + 1];
            const delayMinutes = (scripts[i + 1].delay / 60000).toFixed(1);
            logger.info(`⏳ 等待 ${delayMinutes} 分钟，下一个任务: ${nextScript.name}`);
            await this.delay(scripts[i + 1].delay);
          }
        }
      }

      this.lastRun = new Date();
      const totalTime = (Date.now() - runStartTime.getTime()) / 60000;

      logger.info('='.repeat(60));
      logger.info(`🎉 所有任务执行完成！总耗时: ${totalTime.toFixed(2)} 分钟`);
      logger.info(`⏰ 完成时间: ${new Date().toLocaleString('zh-CN')}`);
      logger.info('='.repeat(60));

      // 如果是立即执行模式，执行完成后退出进程
      if (this.immediateMode) {
        logger.info('🔴 立即执行模式完成，退出进程');
        process.exit(0);
      }

    } catch (error) {
      logger.error('💥 任务序列执行中断:', {
        error: error.message,
        stack: error.stack
      });

      // 立即执行模式下出错也退出进程
      if (this.immediateMode) {
        logger.error('🔴 立即执行模式出错，退出进程');
        process.exit(1);
      }
    } finally {
      this.isRunning = false;
    }
  }

  async executeScript(script) {
    return new Promise((resolve, reject) => {
      const startTime = Date.now();
      const scriptLogger = logger.child({ script: script.name });

      scriptLogger.info(`▶️ 开始执行: ${script.command} ${script.args.join(' ')}`);

      const child = spawn(script.command, script.args, {
        stdio: 'inherit',
        cwd: __dirname
      });

      child.on('close', (code) => {
        const executionTime = ((Date.now() - startTime) / 1000).toFixed(2);
        if (code === 0) {
          scriptLogger.info(`✓ 执行成功 - 耗时: ${executionTime}秒`);
          resolve();
        } else {
          scriptLogger.error(`✗ 执行失败 - 退出码: ${code}, 耗时: ${executionTime}秒`);
          reject(new Error(`脚本退出码: ${code}`));
        }
      });

      child.on('error', (error) => {
        scriptLogger.error(`💥 执行错误: ${error.message}`);
        reject(error);
      });
    });
  }

  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  start() {
    // 如果是指立即执行模式，直接运行一次然后退出
    if (this.immediateMode) {
      logger.info('🔴 立即执行模式启动，开始执行任务序列...');
      this.runScripts();
      return;
    }

    // 正常调度模式
    // 每分钟检查一次执行条件
    setInterval(() => {
      this.runScripts();
    }, 60 * 1000);

    // 启动时立即检查一次
    setTimeout(() => {
      this.runScripts();
    }, 5000); // 延迟5秒启动，确保日志系统就绪

    logger.info('🔍 智能调度器已启动，每分钟检查执行条件');
    logger.info('⏰ 目标执行时间: 每天 8:00 AM');
    logger.info('🔄 错过执行时会自动补偿');
    logger.info('💡 使用 --immediate 参数可以立即执行所有任务');
  }
}

// 添加命令行使用说明
if (require.main === module) {
  const scheduler = new SmartScheduler();

  // 显示帮助信息
  if (process.argv.includes('--help') || process.argv.includes('-h')) {
    console.log(`
Smart Scheduler 使用说明:

正常模式 (后台调度):
  node smartScheduler.js

立即执行模式:
  node smartScheduler.js --immediate

帮助信息:
  node smartScheduler.js --help

功能:
  - 正常模式: 每天 8:00 AM 自动执行任务序列
  - 立即执行模式: 立即执行所有任务，完成后退出
  - 错过执行时会自动补偿执行
    `);
    process.exit(0);
  }

  scheduler.start();
}

module.exports = SmartScheduler;
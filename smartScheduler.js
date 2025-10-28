// smartScheduler.js
const logger = require('./logger');
const { spawn } = require('child_process');

// 直接在这里定义脚本配置（从 scriptRunner.js 复制过来）
const scripts = [
  {
    name: '更新Quote',
    command: 'node',
    args: ['svcUpdateQuote.js', '--immediate'],
    logFile: 'svcCalling.log'
  },
  {
    name: '更新ExchangeRate',
    command: 'node',
    args: ['svcUpdateExchangeRate.js', '--immediate'],
    delay: 5 * 60 * 1000,
    logFile: 'svcCalling.log'
  },
  {
    name: '汇总计算Holding数据',
    command: 'node',
    args: ['svcHoldingAggregationTask.js', '--immediate'],
    delay: 5 * 60 * 1000,
    logFile: 'svcCalling.log'
  },  
  {
    name: '资产负债表更新',
    command: 'node',
    args: ['svcPeriodicalBalanceSheetAll.js', '--immediate'],
    delay: 5 * 60 * 1000,
    logFile: 'svcCalling.log'
  },
  {
    name: 'tblHoldingAggrView导出到Firebase',
    command: 'node', 
    args: ['toolDuckDB2Firebase.js', 'tblHoldingAggrView', 'reports/holdings', '--no-key-check'],
    delay: 2 * 60 * 1000,
    logFile: 'firebase-export.log'
  },
  {
    name: '资产负债表导出到Firebase',
    command: 'node', 
    args: ['toolDuckDB2Firebase.js', 'tblPeriodicBalanceSheet', 'reports/balanceSheet'],
    logFile: 'firebase-export.log'
  }
];

class SmartScheduler {
  constructor() {
    this.lastRun = null;
    this.isRunning = false;
    this.dbLocker = require('./dbLocker'); // 数据库锁
  }

  shouldRun() {
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
      return;
    }

    this.isRunning = true;
    const runStartTime = new Date();
    
    logger.info('='.repeat(60));
    logger.info(`🚀 开始执行每日任务序列 - ${runStartTime.toLocaleString('zh-CN')}`);
    logger.info(`📝 任务数量: ${scripts.length}`);
    logger.info('='.repeat(60));

    try {
      for (let i = 0; i < scripts.length; i++) {
        const script = scripts[i];
        
        // 获取数据库锁
        await this.dbLocker.acquireLock(script.name, 15 * 60 * 1000);
        
        try {
          await this.executeScript(script);
          logger.info(`✅ ${script.name} 完成`);
          
          // 如果不是最后一个脚本，且设置了延迟，则等待
          if (i < scripts.length - 1 && scripts[i + 1].delay) {
            const nextScript = scripts[i + 1];
            const delayMinutes = scripts[i + 1].delay / 60000;
            logger.info(`⏳ 等待 ${delayMinutes} 分钟，下一个任务: ${nextScript.name}`);
            await this.delay(scripts[i + 1].delay);
          }
        } finally {
          // 释放数据库锁
          this.dbLocker.releaseLock(script.name);
        }
      }

      this.lastRun = new Date();
      const totalTime = (Date.now() - runStartTime.getTime()) / 60000;
      
      logger.info('='.repeat(60));
      logger.info(`🎉 所有任务执行完成！总耗时: ${totalTime.toFixed(2)} 分钟`);
      logger.info(`⏰ 完成时间: ${new Date().toLocaleString('zh-CN')}`);
      logger.info('='.repeat(60));

    } catch (error) {
      logger.error('💥 任务序列执行失败:', {
        error: error.message,
        stack: error.stack
      });
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
  }
}

module.exports = SmartScheduler;
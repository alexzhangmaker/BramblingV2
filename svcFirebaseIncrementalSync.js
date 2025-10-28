// svcFirebaseIncrementalSync.js
const duckdb = require('duckdb');
const admin = require('firebase-admin');
const winston = require('winston');
const path = require('path');

// 初始化 Winston 日志
const logDir = path.join(__dirname, 'logs');

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp({
      format: 'YYYY-MM-DD HH:mm:ss'
    }),
    winston.format.errors({ stack: true }),
    winston.format.printf(({ level, message, timestamp, stack, service }) => {
      if (stack) {
        return `${timestamp} [${level.toUpperCase()}] ${service || 'firebase-sync'}: ${message}\n${stack}`;
      }
      return `${timestamp} [${level.toUpperCase()}] ${service || 'firebase-sync'}: ${message}`;
    })
  ),
  defaultMeta: { service: 'firebase-sync' },
  transports: [
    // 文件传输 - 所有日志
    new winston.transports.File({
      filename: path.join(logDir, 'firebase-sync-combined.log'),
      level: 'info',
      maxsize: 10485760, // 10MB
      maxFiles: 5
    }),
    // 文件传输 - 错误日志
    new winston.transports.File({
      filename: path.join(logDir, 'firebase-sync-errors.log'),
      level: 'error',
      maxsize: 10485760, // 10MB
      maxFiles: 5
    }),
    // 文件传输 - 同步操作专用日志
    new winston.transports.File({
      filename: path.join(logDir, 'firebase-sync-operations.log'),
      level: 'info',
      maxsize: 10485760,
      maxFiles: 3
    }),
    // 控制台输出
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.simple()
      )
    })
  ]
});

// 初始化Firebase
const serviceAccount = require('/Users/zhangqing/Documents/Github/serviceKeys/bramblingV2Firebase.json');
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: 'https://outpost-8d74e-14018.firebaseio.com/'
});

const db = admin.database();
const duckDbFilePath = './portfolioData.duckdb';

class IncrementalSyncService {
  constructor() {
    this.dbInstance = new duckdb.Database(duckDbFilePath);
    this.isProcessing = false;
    this.pendingUpdates = new Map(); // 账户ID -> 账户数据
    this.serviceStartTime = new Date();
    
    logger.info('🔄 增量同步服务实例已创建', {
      service: 'firebase-sync',
      startTime: this.serviceStartTime.toISOString()
    });
  }

  createConnection() {
    const connection = this.dbInstance.connect();
    connection.run("PRAGMA threads=2");
    return connection;
  }

  closeConnection(connection) {
    if (connection) {
      try {
        connection.close();
      } catch (error) {
        logger.warn('关闭数据库连接时出现警告', {
          service: 'firebase-sync',
          error: error.message
        });
      }
    }
  }

  async safeRun(connection, query, params = []) {
    return new Promise((resolve, reject) => {
      if (params.length === 0) {
        connection.run(query, (err) => {
          if (err) reject(err);
          else resolve();
        });
      } else {
        connection.run(query, ...params, (err) => {
          if (err) reject(err);
          else resolve();
        });
      }
    });
  }

  /**
   * 处理单个账户的增量更新
   */
  async processAccountUpdate(accountID, accountData) {
    const connection = this.createConnection();
    const syncStartTime = Date.now();
    
    try {
      const holdings = accountData.holdings || {};
      
      logger.info('🔄 开始处理账户增量更新', {
        service: 'firebase-sync',
        accountID: accountID,
        holdingsCount: Object.keys(holdings).length,
        action: 'sync-start'
      });

      // 开始事务
      await this.safeRun(connection, "BEGIN TRANSACTION");

      // 删除该账户的所有现有持仓
      await this.safeRun(connection, "DELETE FROM tblAccountHoldings WHERE accountID = ?", [accountID]);
      
      logger.debug('已清空账户原有持仓数据', {
        service: 'firebase-sync',
        accountID: accountID
      });

      // 插入新的持仓数据
      let insertedCount = 0;
      for (const [holdingKey, holding] of Object.entries(holdings)) {
        await this.safeRun(connection, `
          INSERT INTO tblAccountHoldings 
          (accountID, ticker, company, costPerShare, currency, holding, exchange, exchangeCode, assetClass, description)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `, [
          accountID,
          holding.ticker,
          holding.company || '',
          holding.costPerShare || 0,
          holding.currency || 'USD',
          holding.holding || 0,
          holding.exchange || '',
          holding.exchangeCode || '',
          holding.assetClass || '',
          holding.description || ''
        ]);
        insertedCount++;
      }

      // 记录变更任务
      const taskID = `INCR_${accountID}_${Date.now()}`;
      await this.safeRun(connection, `
        INSERT INTO tblTaskRecords 
        (taskID, taskType, accountID, changeType, newData)
        VALUES (?, 'INCREMENTAL_SYNC', ?, 'FULL_UPDATE', ?)
      `, [taskID, accountID, JSON.stringify({ holdingsCount: Object.keys(holdings).length })]);

      // 提交事务
      await this.safeRun(connection, "COMMIT");

      const syncDuration = Date.now() - syncStartTime;
      
      logger.info('✅ 账户增量更新完成', {
        service: 'firebase-sync',
        accountID: accountID,
        holdingsCount: Object.keys(holdings).length,
        insertedCount: insertedCount,
        duration: `${syncDuration}ms`,
        action: 'sync-complete'
      });

    } catch (error) {
      // 回滚事务
      try {
        await this.safeRun(connection, "ROLLBACK");
      } catch (rollbackError) {
        logger.warn('回滚事务时出错', {
          service: 'firebase-sync',
          accountID: accountID,
          error: rollbackError.message
        });
      }
      
      logger.error('❌ 账户增量更新失败', {
        service: 'firebase-sync',
        accountID: accountID,
        error: error.message,
        stack: error.stack,
        action: 'sync-error'
      });
      
      throw error; // 重新抛出错误以便上层处理
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 处理待更新的队列
   */
  async processPendingUpdates() {
    if (this.isProcessing || this.pendingUpdates.size === 0) {
      return;
    }

    this.isProcessing = true;
    const batchStartTime = Date.now();
    const batchSize = this.pendingUpdates.size;

    try {
      logger.info('📦 开始处理待更新队列', {
        service: 'firebase-sync',
        batchSize: batchSize,
        action: 'batch-start'
      });
      
      const updates = Array.from(this.pendingUpdates.entries());
      this.pendingUpdates.clear();

      let successCount = 0;
      let errorCount = 0;

      // 顺序处理，避免并发冲突
      for (const [accountID, accountData] of updates) {
        try {
          await this.processAccountUpdate(accountID, accountData);
          successCount++;
          // 小延迟，减少数据库压力
          await new Promise(resolve => setTimeout(resolve, 100));
        } catch (error) {
          errorCount++;
          logger.error('处理单个账户更新失败', {
            service: 'firebase-sync',
            accountID: accountID,
            error: error.message
          });
        }
      }

      const batchDuration = Date.now() - batchStartTime;
      
      logger.info('✅ 待更新队列处理完成', {
        service: 'firebase-sync',
        totalCount: batchSize,
        successCount: successCount,
        errorCount: errorCount,
        duration: `${batchDuration}ms`,
        action: 'batch-complete'
      });

    } catch (error) {
      logger.error('❌ 处理待更新队列失败', {
        service: 'firebase-sync',
        batchSize: batchSize,
        error: error.message,
        stack: error.stack,
        action: 'batch-error'
      });
    } finally {
      this.isProcessing = false;
    }
  }

  /**
   * 启动Firebase监听
   */
  startListening() {
    logger.info('👂 启动Firebase增量监听...', {
      service: 'firebase-sync',
      action: 'listener-start'
    });

    // 防抖处理，避免频繁更新
    let processTimer;

    db.ref('accounts').on('child_changed', (snapshot) => {
      const accountID = snapshot.key;
      const accountData = snapshot.val();
      
      logger.info('📢 检测到账户数据变化', {
        service: 'firebase-sync',
        accountID: accountID,
        event: 'child_changed',
        action: 'data-change-detected'
      });
      
      // 添加到待更新队列
      this.pendingUpdates.set(accountID, accountData);
      
      // 防抖处理，2秒后处理更新
      clearTimeout(processTimer);
      processTimer = setTimeout(() => {
        this.processPendingUpdates();
      }, 2000);
    });

    db.ref('accounts').on('child_removed', (snapshot) => {
      const accountID = snapshot.key;
      
      logger.info('🗑️ 检测到账户被删除', {
        service: 'firebase-sync',
        accountID: accountID,
        event: 'child_removed',
        action: 'account-delete-detected'
      });
      
      // 从数据库中删除该账户数据
      this.deleteAccount(accountID);
    });

    db.ref('accounts').on('child_added', (snapshot) => {
      const accountID = snapshot.key;
      
      logger.info('➕ 检测到新账户添加', {
        service: 'firebase-sync',
        accountID: accountID,
        event: 'child_added',
        action: 'account-add-detected'
      });
    });

    logger.info('✅ Firebase增量监听已启动', {
      service: 'firebase-sync',
      action: 'listener-ready'
    });
  }

  /**
   * 删除账户数据
   */
  async deleteAccount(accountID) {
    const connection = this.createConnection();
    const deleteStartTime = Date.now();
    
    try {
      logger.info('开始删除账户数据', {
        service: 'firebase-sync',
        accountID: accountID,
        action: 'delete-start'
      });

      await this.safeRun(connection, "BEGIN TRANSACTION");
      
      await this.safeRun(connection, "DELETE FROM tblAccountHoldings WHERE accountID = ?", [accountID]);
      
      const taskID = `DELETE_${accountID}_${Date.now()}`;
      await this.safeRun(connection, `
        INSERT INTO tblTaskRecords (taskID, taskType, accountID, changeType)
        VALUES (?, 'ACCOUNT_DELETE', ?, 'DELETE')
      `, [taskID, accountID]);
      
      await this.safeRun(connection, "COMMIT");
      
      const deleteDuration = Date.now() - deleteStartTime;
      
      logger.info('✅ 账户数据删除完成', {
        service: 'firebase-sync',
        accountID: accountID,
        duration: `${deleteDuration}ms`,
        action: 'delete-complete'
      });
      
    } catch (error) {
      try {
        await this.safeRun(connection, "ROLLBACK");
      } catch (rollbackError) {
        logger.warn('回滚事务时出错', {
          service: 'firebase-sync',
          accountID: accountID,
          error: rollbackError.message
        });
      }
      
      logger.error('❌ 删除账户数据失败', {
        service: 'firebase-sync',
        accountID: accountID,
        error: error.message,
        stack: error.stack,
        action: 'delete-error'
      });
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 获取服务状态
   */
  getServiceStatus() {
    return {
      isProcessing: this.isProcessing,
      pendingUpdates: this.pendingUpdates.size,
      uptime: Date.now() - this.serviceStartTime.getTime()
    };
  }

  /**
   * 优雅关闭
   */
  async shutdown() {
    const shutdownStartTime = Date.now();
    
    logger.info('🛑 开始停止增量同步服务...', {
      service: 'firebase-sync',
      action: 'shutdown-start',
      pendingUpdates: this.pendingUpdates.size
    });

    // 停止Firebase监听
    db.ref('accounts').off();
    
    logger.info('Firebase监听已停止', {
      service: 'firebase-sync',
      action: 'listener-stopped'
    });

    // 处理剩余更新
    if (this.pendingUpdates.size > 0) {
      logger.info(`处理剩余 ${this.pendingUpdates.size} 个更新...`, {
        service: 'firebase-sync',
        action: 'process-remaining'
      });
      await this.processPendingUpdates();
    }
    
    // 关闭Firebase
    await admin.app().delete();
    
    const shutdownDuration = Date.now() - shutdownStartTime;
    const totalUptime = Date.now() - this.serviceStartTime.getTime();
    
    logger.info('✅ 增量同步服务已停止', {
      service: 'firebase-sync',
      action: 'shutdown-complete',
      shutdownDuration: `${shutdownDuration}ms`,
      totalUptime: `${totalUptime}ms`
    });
  }
}

/**
 * 主函数
 */
async function main() {
  logger.info('🚀 启动Firebase增量同步服务...', {
    service: 'firebase-sync',
    action: 'service-start',
    timestamp: new Date().toISOString()
  });
  
  const syncService = new IncrementalSyncService();
  
  // 注册关闭信号
  process.on('SIGINT', async () => {
    logger.info('收到 SIGINT 信号，开始优雅关闭', {
      service: 'firebase-sync',
      action: 'signal-received',
      signal: 'SIGINT'
    });
    
    await syncService.shutdown();
    logger.info('进程退出', { action: 'process-exit' });
    process.exit(0);
  });
  
  process.on('SIGTERM', async () => {
    logger.info('收到 SIGTERM 信号，开始优雅关闭', {
      service: 'firebase-sync',
      action: 'signal-received',
      signal: 'SIGTERM'
    });
    
    await syncService.shutdown();
    logger.info('进程退出', { action: 'process-exit' });
    process.exit(0);
  });

  process.on('uncaughtException', (error) => {
    logger.error('未捕获的异常', {
      service: 'firebase-sync',
      error: error.message,
      stack: error.stack,
      action: 'uncaught-exception'
    });
    
    // 优雅关闭后退出
    syncService.shutdown().finally(() => {
      process.exit(1);
    });
  });

  process.on('unhandledRejection', (reason, promise) => {
    logger.error('未处理的 Promise 拒绝', {
      service: 'firebase-sync',
      reason: reason instanceof Error ? reason.stack : reason,
      action: 'unhandled-rejection'
    });
  });

  try {
    // 启动监听
    syncService.startListening();
    
    logger.info('✅ 增量同步服务运行中...', {
      service: 'firebase-sync',
      action: 'service-ready'
    });
    
    // 定期记录服务状态
    setInterval(() => {
      const status = syncService.getServiceStatus();
      logger.debug('服务状态心跳', {
        service: 'firebase-sync',
        ...status,
        action: 'heartbeat'
      });
    }, 300000); // 每5分钟记录一次状态
    
  } catch (error) {
    logger.error('❌ 增量同步服务启动失败', {
      service: 'firebase-sync',
      error: error.message,
      stack: error.stack,
      action: 'service-start-error'
    });
    
    await syncService.shutdown();
    process.exit(1);
  }
}

// 运行主函数
if (require.main === module) {
  main().catch(error => {
    logger.error('主函数执行失败', {
      service: 'firebase-sync',
      error: error.message,
      stack: error.stack,
      action: 'main-function-error'
    });
    process.exit(1);
  });
}

module.exports = { IncrementalSyncService, logger };
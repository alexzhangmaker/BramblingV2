// firebaseToDuckDB.js (修复连接管理版本)
const duckdb = require('duckdb');
const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');
const nodeCron = require('node-cron');

// 初始化Firebase
const serviceAccount = require('/Users/zhangqing/Documents/Github/serviceKeys/bramblingV2Firebase.json');
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: 'https://outpost-8d74e-14018.firebaseio.com/'
});

const db = admin.database();
const duckDbFilePath = './portfolioData.duckdb';

// 连接池管理
class ConnectionManager {
  /*
  constructor() {
    this.connections = new Set();
    this.isShuttingDown = false;
  }
  */
  constructor() {
    this.connections = new Set();
    this.isShuttingDown = false;
    this.dbInstance = null;
  }

  getDatabase() {
    if (!this.dbInstance) {
      this.dbInstance = new duckdb.Database(duckDbFilePath);
    }
    return this.dbInstance;
  }

  createConnection() {
    if (this.isShuttingDown) {
      throw new Error('系统正在关闭，无法创建新连接');
    }

    const connection = this.getDatabase().connect();
    
    // 设置更长的超时时间
    connection.run("PRAGMA threads=4");
    connection.run("PRAGMA default_order='asc'");
    
    this.connections.add(connection);
    
    return { connection };
  }

  closeConnection(connectionInfo) {
    if (!connectionInfo || !connectionInfo.connection) return;
    
    const { connection } = connectionInfo;
    
    try {
      // 先提交任何挂起的事务
      connection.run("COMMIT");
    } catch (error) {
      // 忽略提交错误，可能没有活动事务
    }
    
    try {
      connection.close();
      this.connections.delete(connection);
    } catch (error) {
      console.warn('⚠️ 关闭连接时出现警告:', error.message);
    }
  }

  async closeAllConnections() {
    this.isShuttingDown = true;
    console.log('🔒 正在关闭所有数据库连接...');
    
    const closePromises = Array.from(this.connections).map(connection => {
      return new Promise(resolve => {
        try {
          // 尝试提交任何挂起的事务
          try { connection.run("COMMIT"); } catch (e) {}
          connection.close();
          resolve();
        } catch (error) {
          console.warn('关闭连接时出错:', error.message);
          resolve();
        }
      });
    });
    
    await Promise.all(closePromises);
    this.connections.clear();
    
    if (this.dbInstance) {
      try {
        this.dbInstance.close();
        this.dbInstance = null;
      } catch (error) {
        console.warn('关闭数据库实例时出错:', error.message);
      }
    }
    
    console.log('✅ 所有数据库连接已关闭');
  }
}

// 创建全局连接管理器
const connectionManager = new ConnectionManager();

/**
 * 安全转换函数 - 处理BigInt和其他数据类型
 */
function safeConvert(value) {
  if (value === null || value === undefined) {
    return 0;
  }
  if (typeof value === 'bigint') {
    return Number(value);
  }
  if (typeof value === 'number') {
    return value;
  }
  if (typeof value === 'string') {
    const num = parseFloat(value);
    return isNaN(num) ? 0 : num;
  }
  return Number(value) || 0;
}

/**
 * 创建DuckDB数据库连接（使用连接管理器）
 */
function createDuckDBConnection() {
  return connectionManager.createConnection();
}


/**
 * 安全的DuckDB查询函数 - 改进版本
 */
function safeDuckDBQuery(connection, query, params = []) {
  return new Promise((resolve, reject) => {
    // 设置查询超时
    const timeout = setTimeout(() => {
      reject(new Error('查询超时'));
    }, 30000);

    const executeQuery = () => {
      if (params.length === 0) {
        connection.all(query, (err, result) => {
          clearTimeout(timeout);
          if (err) {
            reject(err);
          } else {
            const convertedResult = (Array.isArray(result) ? result : []).map(row => {
              const convertedRow = {};
              for (const [key, value] of Object.entries(row)) {
                convertedRow[key] = typeof value === 'bigint' ? Number(value) : value;
              }
              return convertedRow;
            });
            resolve(convertedResult);
          }
        });
      } else {
        connection.all(query, ...params, (err, result) => {
          clearTimeout(timeout);
          if (err) {
            reject(err);
          } else {
            const convertedResult = (Array.isArray(result) ? result : []).map(row => {
              const convertedRow = {};
              for (const [key, value] of Object.entries(row)) {
                convertedRow[key] = typeof value === 'bigint' ? Number(value) : value;
              }
              return convertedRow;
            });
            resolve(convertedResult);
          }
        });
      }
    };

    // 如果连接繁忙，稍后重试
    try {
      executeQuery();
    } catch (error) {
      clearTimeout(timeout);
      reject(error);
    }
  });
}

/**
 * 安全的DuckDB执行函数 - 改进版本
 */
function safeDuckDBRun(connection, query, params = []) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error('执行超时'));
    }, 30000);

    const executeRun = () => {
      if (params.length === 0) {
        connection.run(query, (err) => {
          clearTimeout(timeout);
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      } else {
        connection.run(query, ...params, (err) => {
          clearTimeout(timeout);
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      }
    };

    try {
      executeRun();
    } catch (error) {
      clearTimeout(timeout);
      reject(error);
    }
  });
}

/**
 * 批量处理持仓数据 - 减少事务冲突
 */
async function processAccountHoldingsBatch(accountID, accountData) {
  const connectionInfo = createDuckDBConnection();
  
  try {
    const holdings = accountData.holdings || {};
    const holdingsArray = Object.entries(holdings);
    
    console.log(`📦 批量处理账户 ${accountID} 的 ${holdingsArray.length} 个持仓`);

    // 开始事务
    await safeDuckDBRun(connectionInfo.connection, "BEGIN TRANSACTION");

    let successCount = 0;
    let errorCount = 0;

    for (const [holdingKey, holding] of holdingsArray) {
      try {
        // 使用单个事务处理所有插入
        await safeDuckDBRun(connectionInfo.connection, `
          INSERT OR REPLACE INTO tblAccountHoldings 
          (accountID, ticker, company, costPerShare, currency, holding, exchange, exchangeCode, assetClass, description, lastUpdated)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        `, [
          accountID,
          holding.ticker,
          holding.company || '',
          safeConvert(holding.costPerShare),
          holding.currency || 'USD',
          safeConvert(holding.holding),
          holding.exchange || '',
          holding.exchangeCode || '',
          holding.assetClass || '',
          holding.description || ''
        ]);

        // 记录数据变更任务
        const taskID = `TASK_${accountID}_${holding.ticker}_${Date.now()}`;
        await safeDuckDBRun(connectionInfo.connection, `
          INSERT INTO tblTaskRecords 
          (taskID, taskType, accountID, ticker, changeType, newData)
          VALUES (?, 'HOLDING_UPDATE', ?, ?, 'UPSERT', ?)
        `, [
          taskID,
          accountID,
          holding.ticker,
          JSON.stringify(holding)
        ]);

        successCount++;
        
        if (successCount % 10 === 0) {
          console.log(`✅ 已处理 ${successCount}/${holdingsArray.length} 个持仓`);
        }

      } catch (error) {
        errorCount++;
        console.error(`❌ 处理持仓失败 ${accountID}-${holding.ticker}:`, error.message);
        // 继续处理其他持仓，不中断整个批次
      }
    }

    // 提交事务
    await safeDuckDBRun(connectionInfo.connection, "COMMIT");
    
    console.log(`🎯 账户 ${accountID} 处理完成: ${successCount} 成功, ${errorCount} 失败`);

  } catch (error) {
    // 回滚事务
    try {
      await safeDuckDBRun(connectionInfo.connection, "ROLLBACK");
    } catch (rollbackError) {
      console.warn('回滚事务时出错:', rollbackError.message);
    }
    
    console.error(`❌ 处理账户 ${accountID} 数据失败:`, error.message);
  } finally {
    connectionManager.closeConnection(connectionInfo);
  }
}



/**
 * 检查并更新表结构 - 安全版本
 */
async function updateTableStructure() {
  const connectionInfo = createDuckDBConnection();
  
  try {
    console.log('🔧 检查并更新表结构...');
    
    // 只重新创建汇总表（不包含重要数据）
    await safeDuckDBRun(connectionInfo.connection, 'DROP TABLE IF EXISTS tblHoldingAggrView');
    
    await safeDuckDBRun(connectionInfo.connection, `
      CREATE TABLE tblHoldingAggrView (
        ticker VARCHAR PRIMARY KEY,
        totalHolding INTEGER,
        avgCostPrice DOUBLE,
        totalCost DOUBLE,
        currentPrice DOUBLE,
        costCNY DOUBLE,
        valueCNY DOUBLE,
        PLRatio DOUBLE,
        costInTotal DOUBLE,
        valueInTotal DOUBLE,
        accountCount INTEGER DEFAULT 1,
        currency VARCHAR,
        calculatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    console.log('✅ 重新创建 tblHoldingAggrView 表');
    
    // 对于其他表，只检查缺失的列，不重新创建表
    const tablesToCheck = [
      {
        name: 'tblAccountHoldings',
        columns: [
          { name: 'exchange', type: 'VARCHAR' },
          { name: 'exchangeCode', type: 'VARCHAR' },
          { name: 'assetClass', type: 'VARCHAR' },
          { name: 'description', type: 'VARCHAR' }
        ]
      },
      {
        name: 'tblTaskRecords',
        columns: []
      },
      {
        name: 'tblQuotationTTM',
        columns: []
      },
      {
        name: 'tblExchangeRateTTM',
        columns: []
      }
    ];
    
    for (const table of tablesToCheck) {
      const tableExists = await safeDuckDBQuery(connectionInfo.connection, `
        SELECT COUNT(*) as count FROM information_schema.tables 
        WHERE table_name = '${table.name.toLowerCase()}'
      `);
      
      if (tableExists[0].count === 0) {
        // 表不存在，创建它
        let createSQL = '';
        if (table.name === 'tblAccountHoldings') {
          createSQL = `
            CREATE TABLE tblAccountHoldings (
              accountID VARCHAR,
              ticker VARCHAR,
              company VARCHAR,
              costPerShare DOUBLE,
              currency VARCHAR,
              holding INTEGER,
              exchange VARCHAR,
              exchangeCode VARCHAR,
              assetClass VARCHAR,
              description VARCHAR,
              lastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (accountID, ticker)
            )
          `;
        } else if (table.name === 'tblTaskRecords') {
          createSQL = `
            CREATE TABLE tblTaskRecords (
              taskID VARCHAR PRIMARY KEY,
              taskType VARCHAR,
              accountID VARCHAR,
              ticker VARCHAR,
              changeType VARCHAR,
              oldData JSON,
              newData JSON,
              processed BOOLEAN DEFAULT FALSE,
              createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
          `;
        } else if (table.name === 'tblQuotationTTM') {
          createSQL = `
            CREATE TABLE tblQuotationTTM (
              ticker VARCHAR PRIMARY KEY,
              price DOUBLE,
              currency VARCHAR,
              lastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
          `;
        } else if (table.name === 'tblExchangeRateTTM') {
          createSQL = `
            CREATE TABLE tblExchangeRateTTM (
              fromCurrency VARCHAR,
              toCurrency VARCHAR,
              rate DOUBLE,
              lastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (fromCurrency, toCurrency)
            )
          `;
        }
        
        if (createSQL) {
          await safeDuckDBRun(connectionInfo.connection, createSQL);
          console.log(`✅ 创建表 ${table.name}`);
        }
      } else {
        // 表已存在，只检查缺失的列
        const existingColumns = await safeDuckDBQuery(connectionInfo.connection, `
          SELECT column_name 
          FROM information_schema.columns 
          WHERE table_name = '${table.name.toLowerCase()}'
        `);
        
        const existingColumnNames = existingColumns.map(col => col.column_name.toLowerCase());
        
        for (const column of table.columns) {
          if (!existingColumnNames.includes(column.name.toLowerCase())) {
            console.log(`📝 为表 ${table.name} 添加缺失的列: ${column.name}`);
            await safeDuckDBRun(connectionInfo.connection, `
              ALTER TABLE ${table.name} ADD COLUMN ${column.name} ${column.type}
            `);
            console.log(`✅ 成功添加列: ${column.name}`);
          }
        }
      }
    }
    
    console.log('✅ 所有表结构更新完成');
  } catch (error) {
    console.error('❌ 表结构更新失败:', error.message);
    // 不抛出错误，让系统继续运行
  } finally {
    connectionManager.closeConnection(connectionInfo);
  }
}

/**
 * 初始化数据库表结构 - 优化版本
 */
async function initializeDatabase() {
  const connectionInfo = createDuckDBConnection();
  
  try {
    console.log('🗄️ 开始初始化数据库表结构...');

    // 设置数据库优化参数
    await safeDuckDBRun(connectionInfo.connection, "PRAGMA threads=4");
    await safeDuckDBRun(connectionInfo.connection, "PRAGMA default_order='asc'");
    await safeDuckDBRun(connectionInfo.connection, "PRAGMA memory_limit='1GB'");

    // 创建或更新表结构
    await safeDuckDBRun(connectionInfo.connection, `
      CREATE TABLE IF NOT EXISTS tblAccountHoldings (
        accountID VARCHAR,
        ticker VARCHAR,
        company VARCHAR,
        costPerShare DOUBLE,
        currency VARCHAR,
        holding INTEGER,
        exchange VARCHAR,
        exchangeCode VARCHAR,
        assetClass VARCHAR,
        description VARCHAR,
        lastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (accountID, ticker)
      )
    `);

    await safeDuckDBRun(connectionInfo.connection, `
      CREATE TABLE IF NOT EXISTS tblTaskRecords (
        taskID VARCHAR PRIMARY KEY,
        taskType VARCHAR,
        accountID VARCHAR,
        ticker VARCHAR,
        changeType VARCHAR,
        oldData JSON,
        newData JSON,
        processed BOOLEAN DEFAULT FALSE,
        createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await safeDuckDBRun(connectionInfo.connection, `
      CREATE TABLE IF NOT EXISTS tblQuotationTTM (
        ticker VARCHAR PRIMARY KEY,
        price DOUBLE,
        currency VARCHAR,
        lastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await safeDuckDBRun(connectionInfo.connection, `
      CREATE TABLE IF NOT EXISTS tblExchangeRateTTM (
        fromCurrency VARCHAR,
        toCurrency VARCHAR,
        rate DOUBLE,
        lastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (fromCurrency, toCurrency)
      )
    `);

    await safeDuckDBRun(connectionInfo.connection, `
      CREATE TABLE IF NOT EXISTS tblHoldingAggrView (
        ticker VARCHAR PRIMARY KEY,
        totalHolding INTEGER,
        avgCostPrice DOUBLE,
        totalCost DOUBLE,
        currentPrice DOUBLE,
        costCNY DOUBLE,
        valueCNY DOUBLE,
        PLRatio DOUBLE,
        costInTotal DOUBLE,
        valueInTotal DOUBLE,
        calculatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    console.log('✅ 所有数据库表初始化完成');

    // 创建索引以提高性能
    try {
      await safeDuckDBRun(connectionInfo.connection, "CREATE INDEX IF NOT EXISTS idx_account_ticker ON tblAccountHoldings(accountID, ticker)");
      await safeDuckDBRun(connectionInfo.connection, "CREATE INDEX IF NOT EXISTS idx_ticker ON tblAccountHoldings(ticker)");
      console.log('✅ 数据库索引创建完成');
    } catch (error) {
      console.warn('⚠️ 创建索引时出现警告:', error.message);
    }

  } catch (error) {
    console.error('❌ 数据库初始化失败:', error.message);
  } finally {
    connectionManager.closeConnection(connectionInfo);
  }
}

/**
 * 从Firebase读取所有账户数据
 */
async function fetchAllAccountsFromFirebase() {
  try {
    const snapshot = await db.ref('accounts').once('value');
    const accounts = snapshot.val();
    console.log(`📊 从Firebase读取到 ${Object.keys(accounts).length} 个账户`);
    return accounts;
  } catch (error) {
    console.error('❌ 从Firebase读取数据失败:', error);
    return {};
  }
}

/**
 * 处理单个账户的持仓数据
 */
/*
async function processAccountHoldings(accountID, accountData) {
  const connectionInfo = createDuckDBConnection();
  
  try {
    const holdings = accountData.holdings || {};
    
    for (const [holdingKey, holding] of Object.entries(holdings)) {
      try {
        await safeDuckDBRun(connectionInfo.connection, `
          INSERT OR REPLACE INTO tblAccountHoldings 
          (accountID, ticker, company, costPerShare, currency, holding, exchange, exchangeCode, assetClass, description, lastUpdated)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        `, [
          accountID,
          holding.ticker,
          holding.company || '',
          safeConvert(holding.costPerShare),
          holding.currency || 'USD',
          safeConvert(holding.holding),
          holding.exchange || '',
          holding.exchangeCode || '',
          holding.assetClass || '',
          holding.description || ''
        ]);

        const taskID = `TASK_${accountID}_${holding.ticker}_${Date.now()}`;
        await safeDuckDBRun(connectionInfo.connection, `
          INSERT INTO tblTaskRecords 
          (taskID, taskType, accountID, ticker, changeType, newData)
          VALUES (?, 'HOLDING_UPDATE', ?, ?, 'UPSERT', ?)
        `, [
          taskID,
          accountID,
          holding.ticker,
          JSON.stringify(holding)
        ]);

        console.log(`✅ 处理持仓: ${accountID} - ${holding.ticker} [${holding.exchangeCode}]`);
      } catch (error) {
        console.error(`❌ 处理持仓失败 ${accountID}-${holding.ticker}:`, error.message);
      }
    }

  } catch (error) {
    console.error(`❌ 处理账户 ${accountID} 数据失败:`, error.message);
  } finally {
    connectionManager.closeConnection(connectionInfo);
  }
}
*/
async function processAccountHoldings(accountID, accountData) {
  return processAccountHoldingsBatch(accountID, accountData);
}

/**
 * 改进的Firebase监听 - 防抖处理
 */
function setupFirebaseListener() {
  console.log('👂 开始监听Firebase数据变化...');
  
  let processing = false;
  let pendingUpdate = false;

  const processUpdates = async () => {
    if (processing) {
      pendingUpdate = true;
      return;
    }

    processing = true;
    
    try {
      console.log('🔄 Firebase数据发生变化，开始同步...');
      const snapshot = await db.ref('accounts').once('value');
      const accounts = snapshot.val();
      
      // 顺序处理账户，减少并发冲突
      for (const [accountID, accountData] of Object.entries(accounts)) {
        if (accountData && accountData.holdings) {
          await processAccountHoldingsBatch(accountID, accountData);
          // 添加小延迟，减少数据库压力
          await new Promise(resolve => setTimeout(resolve, 100));
        }
      }
      
      console.log('✅ Firebase数据同步完成');
    } catch (error) {
      console.error('❌ Firebase数据同步失败:', error.message);
    } finally {
      processing = false;
      
      if (pendingUpdate) {
        pendingUpdate = false;
        setTimeout(processUpdates, 1000); // 1秒后处理待更新
      }
    }
  };

  // 使用防抖，避免频繁更新
  let updateTimeout;
  db.ref('accounts').on('value', (snapshot) => {
    clearTimeout(updateTimeout);
    updateTimeout = setTimeout(processUpdates, 2000); // 2秒防抖
  });

  // 监听特定账户的变化
  db.ref('accounts').on('child_changed', (snapshot) => {
    const accountID = snapshot.key;
    const accountData = snapshot.val();
    console.log(`🔄 账户 ${accountID} 数据发生变化`);
    
    if (accountData && accountData.holdings) {
      processAccountHoldingsBatch(accountID, accountData);
    }
  });
}

/**
 * 任务调度框架
 */
class TaskScheduler {
  constructor() {
    this.tasks = new Map();
  }

  registerTask(taskName, cronExpression, taskFunction) {
    this.tasks.set(taskName, {
      cronExpression,
      taskFunction,
      scheduled: false
    });
    console.log(`✅ 注册任务: ${taskName}`);
  }

  startAllTasks() {
    this.tasks.forEach((task, taskName) => {
      if (!task.scheduled) {
        nodeCron.schedule(task.cronExpression, () => {
          console.log(`🚀 执行任务: ${taskName}`);
          try {
            task.taskFunction();
          } catch (error) {
            console.error(`❌ 任务 ${taskName} 执行失败:`, error);
          }
        });
        task.scheduled = true;
        console.log(`✅ 启动任务: ${taskName}`);
      }
    });
  }

  executeTaskImmediately(taskName) {
    const task = this.tasks.get(taskName);
    if (task && task.taskFunction) {
      console.log(`⚡ 立即执行任务: ${taskName}`);
      task.taskFunction();
    }
  }
}

// 创建任务调度器实例
const taskScheduler = new TaskScheduler();

/**
 * 初始化示例数据
 */
async function initializeSampleData() {
  const connectionInfo = createDuckDBConnection();
  
  try {
    console.log('📝 开始初始化示例数据...');

    await safeDuckDBRun(connectionInfo.connection, 'DELETE FROM tblQuotationTTM');
    await safeDuckDBRun(connectionInfo.connection, 'DELETE FROM tblExchangeRateTTM');

    const sampleQuotations = [
      { ticker: '0006.HK', price: 28.5, currency: 'HKD' },
      { ticker: '600519.SS', price: 1600.0, currency: 'CNY' },
      { ticker: 'APO', price: 105.25, currency: 'USD' },
      { ticker: 'BAM', price: 52.75, currency: 'USD' },
      { ticker: 'BN', price: 54.25, currency: 'USD' }
    ];
    
    for (const quote of sampleQuotations) {
      await safeDuckDBRun(connectionInfo.connection, `
        INSERT INTO tblQuotationTTM (ticker, price, currency)
        VALUES (?, ?, ?)
      `, [quote.ticker, quote.price, quote.currency]);
      console.log(`✅ 添加报价: ${quote.ticker} - ${quote.price} ${quote.currency}`);
    }

    const sampleRates = [
      { fromCurrency: 'HKD', toCurrency: 'CNY', rate: 0.92 },
      { fromCurrency: 'USD', toCurrency: 'CNY', rate: 7.25 },
      { fromCurrency: 'CNY', toCurrency: 'CNY', rate: 1.0 }
    ];
    
    for (const rate of sampleRates) {
      await safeDuckDBRun(connectionInfo.connection, `
        INSERT INTO tblExchangeRateTTM (fromCurrency, toCurrency, rate)
        VALUES (?, ?, ?)
      `, [rate.fromCurrency, rate.toCurrency, rate.rate]);
      console.log(`✅ 添加汇率: ${rate.fromCurrency}->${rate.toCurrency} = ${rate.rate}`);
    }

    console.log('✅ 示例数据初始化完成');
  } catch (error) {
    console.error('❌ 示例数据初始化失败:', error.message);
  } finally {
    connectionManager.closeConnection(connectionInfo);
  }
}


/**
 * 汇总任务1: 持仓汇总计算 - 安全版本
 */
async function createHoldingAggregationTask() {
  const connectionInfo = createDuckDBConnection();
  
  try {
    console.log('📈 开始执行持仓汇总任务...');
    
    // 首先确保表结构是最新的（但不中断执行）
    try {
      await updateTableStructure();
    } catch (updateError) {
      console.warn('⚠️ 表结构更新有警告，但继续执行汇总任务:', updateError.message);
    }
    
    // 步骤1: 按ticker汇总持仓
    const holdingSummary = await safeDuckDBQuery(connectionInfo.connection, `
      SELECT 
        ticker,
        SUM(holding) as totalHolding,
        AVG(costPerShare) as avgCostPrice,
        SUM(holding * costPerShare) as totalCost,
        currency,
        COUNT(DISTINCT accountID) as accountCount
      FROM tblAccountHoldings 
      WHERE ticker NOT LIKE 'CASH_%'
      GROUP BY ticker, currency
      ORDER BY totalCost DESC
    `);

    console.log(`📊 找到 ${holdingSummary.length} 个唯一持仓记录进行汇总`);

    if (holdingSummary.length === 0) {
      console.log('⚠️ 没有找到持仓数据，跳过汇总');
      return;
    }

    // 步骤2: 获取实时报价和汇率
    const quotations = await safeDuckDBQuery(connectionInfo.connection, "SELECT ticker, price, currency FROM tblQuotationTTM");
    const exchangeRates = await safeDuckDBQuery(connectionInfo.connection, "SELECT fromCurrency, toCurrency, rate FROM tblExchangeRateTTM WHERE toCurrency = 'CNY'");
    
    console.log(`📊 获取到 ${quotations.length} 个报价记录`);
    console.log(`📊 获取到 ${exchangeRates.length} 个汇率记录`);

    const quoteMap = new Map(quotations.map(q => [q.ticker, q]));
    const rateMap = new Map(exchangeRates.map(r => [`${r.fromCurrency}_${r.toCurrency}`, r.rate]));
    
    // 步骤3: 计算各项指标
    let totalCostCNY = 0;
    let totalValueCNY = 0;
    
    const aggregatedData = holdingSummary.map(holding => {
      const quote = quoteMap.get(holding.ticker) || { price: 0, currency: holding.currency };
      const exchangeRateKey = `${holding.currency}_CNY`;
      const exchangeRate = safeConvert(rateMap.get(exchangeRateKey)) || 1;
      
      const totalHolding = safeConvert(holding.totalHolding);
      const totalCost = safeConvert(holding.totalCost);
      const quotePrice = safeConvert(quote.price);
      
      const costCNY = totalCost * exchangeRate;
      const valueCNY = totalHolding * quotePrice * exchangeRate;
      
      totalCostCNY += costCNY;
      totalValueCNY += valueCNY;
      
      return {
        ticker: holding.ticker,
        totalHolding: totalHolding,
        avgCostPrice: safeConvert(holding.avgCostPrice),
        totalCost: totalCost,
        currentPrice: quotePrice,
        costCNY,
        valueCNY,
        PLRatio: costCNY > 0 ? ((valueCNY - costCNY) / costCNY) * 100 : 0,
        costInTotal: 0,
        valueInTotal: 0,
        accountCount: holding.accountCount,
        currency: holding.currency
      };
    });

    // 步骤4: 计算占比
    const finalData = aggregatedData.map(item => ({
      ...item,
      costInTotal: totalCostCNY > 0 ? (item.costCNY / totalCostCNY) * 100 : 0,
      valueInTotal: totalValueCNY > 0 ? (item.valueCNY / totalValueCNY) * 100 : 0
    }));

    // 步骤5: 保存到汇总表
    console.log('💾 保存汇总数据到数据库...');
    
    try {
      await safeDuckDBRun(connectionInfo.connection, 'BEGIN TRANSACTION');
      
      // 先清空表
      await safeDuckDBRun(connectionInfo.connection, 'DELETE FROM tblHoldingAggrView');
      
      // 批量插入数据
      for (const item of finalData) {
        await safeDuckDBRun(connectionInfo.connection, `
          INSERT INTO tblHoldingAggrView 
          (ticker, totalHolding, avgCostPrice, totalCost, currentPrice, costCNY, valueCNY, PLRatio, costInTotal, valueInTotal, accountCount, currency)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `, [
          item.ticker,
          item.totalHolding,
          item.avgCostPrice,
          item.totalCost,
          item.currentPrice,
          item.costCNY,
          item.valueCNY,
          item.PLRatio,
          item.costInTotal,
          item.valueInTotal,
          item.accountCount,
          item.currency
        ]);
      }
      
      await safeDuckDBRun(connectionInfo.connection, 'COMMIT');
      console.log(`✅ 成功插入 ${finalData.length} 个汇总记录`);
      
    } catch (transactionError) {
      await safeDuckDBRun(connectionInfo.connection, 'ROLLBACK');
      console.error('❌ 事务失败，已回滚:', transactionError.message);
      // 不抛出错误，让系统继续运行
    }

    console.log(`✅ 持仓汇总完成，处理了 ${finalData.length} 个标的`);
    console.log(`💰 总成本: ${totalCostCNY.toFixed(2)} CNY, 总市值: ${totalValueCNY.toFixed(2)} CNY`);

    // 显示汇总结果
    console.log('\n📋 汇总结果 (前10个):');
    finalData.slice(0, 10).forEach(item => {
      console.log(`   ${item.ticker}: ${item.totalHolding}股 @ ${item.avgCostPrice.toFixed(2)} ${item.currency} [${item.accountCount}个账户] - 成本 ${item.costCNY.toFixed(2)} CNY, 市值 ${item.valueCNY.toFixed(2)} CNY, 损益 ${item.PLRatio.toFixed(2)}%`);
    });

    if (finalData.length > 10) {
      console.log(`   ... 还有 ${finalData.length - 10} 个标的`);
    }

    // 显示跨账户持仓统计
    const crossAccountHoldings = finalData.filter(item => item.accountCount > 1);
    if (crossAccountHoldings.length > 0) {
      console.log(`\n🔀 跨账户持仓 (${crossAccountHoldings.length} 个):`);
      crossAccountHoldings.forEach(item => {
        console.log(`   ${item.ticker}: 在 ${item.accountCount} 个账户中持有 ${item.totalHolding} 股`);
      });
    }

  } catch (error) {
    console.error('❌ 持仓汇总任务失败:', error.message);
    console.error('错误详情:', error);
  } finally {
    connectionManager.closeConnection(connectionInfo);
  }
}

/**
 * 检查数据库状态
 */
async function checkDatabaseStatus() {
  const connectionInfo = createDuckDBConnection();
  
  try {
    console.log('\n🔍 检查数据库状态...');
    
    const tableCounts = await safeDuckDBQuery(connectionInfo.connection, `
      SELECT 
        (SELECT COUNT(*) FROM tblAccountHoldings) as holdings_count,
        (SELECT COUNT(*) FROM tblQuotationTTM) as quotations_count,
        (SELECT COUNT(*) FROM tblExchangeRateTTM) as rates_count,
        (SELECT COUNT(*) FROM tblHoldingAggrView) as aggr_count
    `);
    
    console.log('📊 数据库统计:');
    console.log(`  持仓记录: ${tableCounts[0]?.holdings_count || 0}`);
    console.log(`  报价记录: ${tableCounts[0]?.quotations_count || 0}`);
    console.log(`  汇率记录: ${tableCounts[0]?.rates_count || 0}`);
    console.log(`  汇总记录: ${tableCounts[0]?.aggr_count || 0}`);
    
    const holdings = await safeDuckDBQuery(connectionInfo.connection, 'SELECT accountID, ticker, holding, costPerShare, currency, exchangeCode, assetClass FROM tblAccountHoldings');
    console.log('\n📋 当前持仓:');
    holdings.forEach(h => {
      console.log(`  ${h.accountID} - ${h.ticker}: ${h.holding}股 @ ${h.costPerShare} ${h.currency} [${h.exchangeCode}] - ${h.assetClass}`);
    });

  } catch (error) {
    console.error('❌ 数据库状态检查失败:', error.message);
  } finally {
    connectionManager.closeConnection(connectionInfo);
  }
}

/**
 * 优雅关闭函数
 */
/*
async function gracefulShutdown() {
  console.log('\n🛑 开始优雅关闭系统...');
  
  try {
    // 1. 停止Firebase监听
    db.ref('accounts').off();
    console.log('✅ Firebase监听已停止');
    
    // 2. 关闭所有数据库连接
    await connectionManager.closeAllConnections();
    
    // 3. 关闭Firebase应用
    await admin.app().delete();
    console.log('✅ Firebase连接已关闭');
    
    console.log('🎉 系统已安全关闭');
    process.exit(0);
  } catch (error) {
    console.error('❌ 关闭过程中出错:', error.message);
    process.exit(1);
  }
}
*/
/**
 * 优雅关闭函数
 */
async function gracefulShutdown() {
  console.log('\n🛑 开始优雅关闭系统...');
  
  try {
    // 1. 停止Firebase监听
    db.ref('accounts').off();
    console.log('✅ Firebase监听已停止');
    
    // 2. 关闭所有数据库连接
    await connectionManager.closeAllConnections();
    
    // 3. 关闭Firebase应用
    await admin.app().delete();
    console.log('✅ Firebase连接已关闭');
    
    console.log('🎉 系统已安全关闭');
    process.exit(0);
  } catch (error) {
    console.error('❌ 关闭过程中出错:', error.message);
    process.exit(1);
  }
}


/**
 * 主函数
 */
async function main() {
  console.log('🚀 启动Firebase到DuckDB数据同步系统...');
  
  // 注册关闭信号处理
  process.on('SIGINT', gracefulShutdown);
  process.on('SIGTERM', gracefulShutdown);
  process.on('SIGQUIT', gracefulShutdown);
  
  // 处理未捕获的异常
  process.on('uncaughtException', (error) => {
    console.error('💥 未捕获的异常:', error);
    gracefulShutdown();
  });
  
  process.on('unhandledRejection', (reason, promise) => {
    console.error('💥 未处理的Promise拒绝:', reason);
    gracefulShutdown();
  });

  try {
    await initializeDatabase();
    await initializeSampleData();
    
    console.log('🔄 首次从Firebase同步数据...');
    const accounts = await fetchAllAccountsFromFirebase();
    for (const [accountID, accountData] of Object.entries(accounts)) {
      if (accountData && accountData.holdings) {
        await processAccountHoldingsBatch(accountID, accountData);
      }
    }
    
    await checkDatabaseStatus();
    setupFirebaseListener();
    
    taskScheduler.registerTask(
      'holdingAggregation', 
      '0 0 18 * * *',
      createHoldingAggregationTask
    );
    
    taskScheduler.startAllTasks();
    
    setTimeout(async () => {
      await createHoldingAggregationTask();
    }, 2000);
    
    console.log('✅ 系统启动完成，开始运行...');
    console.log('💡 使用 Ctrl+C 来优雅关闭系统');
    
  } catch (error) {
    console.error('❌ 系统启动失败:', error.message);
    await gracefulShutdown();
  }
}

// 启动系统
main().catch(async (error) => {
  console.error('💥 系统崩溃:', error);
  await gracefulShutdown();
});
// firebaseToDuckDB.js (修复版)
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
 * 创建DuckDB数据库连接
 */
function createDuckDBConnection() {
  const duckDb = new duckdb.Database(duckDbFilePath);
  const connection = duckDb.connect();
  return { duckDb, connection };
}

/**
 * 安全的DuckDB查询函数
 */
function safeDuckDBQuery(connection, query, params = []) {
  return new Promise((resolve, reject) => {
    if (params.length === 0) {
      connection.all(query, (err, result) => {
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
  });
}

/**
 * 安全的DuckDB执行函数
 */
function safeDuckDBRun(connection, query, params = []) {
  return new Promise((resolve, reject) => {
    if (params.length === 0) {
      connection.run(query, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    } else {
      connection.run(query, ...params, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    }
  });
}

/**
 * 检查并更新表结构
 */
async function updateTableStructure() {
  const { connection } = createDuckDBConnection();
  
  try {
    console.log('🔧 检查并更新表结构...');
    
    // 检查表是否存在
    const tableExists = await safeDuckDBQuery(connection, `
      SELECT COUNT(*) as count FROM information_schema.tables 
      WHERE table_name = 'tblaccountholdings'
    `);
    
    if (tableExists[0].count > 0) {
      // 表已存在，检查并添加缺失的列
      const columns = await safeDuckDBQuery(connection, `
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'tblaccountholdings'
      `);
      
      const existingColumns = columns.map(col => col.column_name.toLowerCase());
      const requiredColumns = ['exchange', 'exchangecode', 'assetclass', 'description'];
      
      for (const column of requiredColumns) {
        if (!existingColumns.includes(column.toLowerCase())) {
          console.log(`📝 添加缺失的列: ${column}`);
          let columnType = 'VARCHAR';
          if (column === 'assetclass') columnType = 'VARCHAR';
          
          await safeDuckDBRun(connection, `
            ALTER TABLE tblAccountHoldings ADD COLUMN ${column} ${columnType}
          `);
          console.log(`✅ 成功添加列: ${column}`);
        }
      }
    } else {
      // 表不存在，创建新表
      await safeDuckDBRun(connection, `
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
      `);
      console.log('✅ 创建新表 tblAccountHoldings');
    }
    
    console.log('✅ 表结构更新完成');
  } catch (error) {
    console.error('❌ 表结构更新失败:', error.message);
    throw error;
  } finally {
    connection.close();
  }
}

/**
 * 初始化数据库表结构
 */
async function initializeDatabase() {
  try {
    console.log('🗄️ 开始初始化数据库表结构...');

    // 先更新表结构
    await updateTableStructure();

    // 创建其他表
    const { connection } = createDuckDBConnection();
    
    await safeDuckDBRun(connection, `
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
    console.log('✅ 创建 tblTaskRecords 表');

    await safeDuckDBRun(connection, `
      CREATE TABLE IF NOT EXISTS tblQuotationTTM (
        ticker VARCHAR PRIMARY KEY,
        price DOUBLE,
        currency VARCHAR,
        lastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    console.log('✅ 创建 tblQuotationTTM 表');

    await safeDuckDBRun(connection, `
      CREATE TABLE IF NOT EXISTS tblExchangeRateTTM (
        fromCurrency VARCHAR,
        toCurrency VARCHAR,
        rate DOUBLE,
        lastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (fromCurrency, toCurrency)
      )
    `);
    console.log('✅ 创建 tblExchangeRateTTM 表');

    await safeDuckDBRun(connection, `
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
    console.log('✅ 创建 tblHoldingAggrView 表');

    console.log('✅ 所有数据库表初始化完成');
    connection.close();
  } catch (error) {
    console.error('❌ 数据库初始化失败:', error.message);
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
 * 处理单个账户的持仓数据 - 更新以适应新数据结构
 */
async function processAccountHoldings(accountID, accountData) {
  const { connection } = createDuckDBConnection();
  
  try {
    const holdings = accountData.holdings || {};
    
    for (const [holdingKey, holding] of Object.entries(holdings)) {
      try {
        // 使用INSERT OR REPLACE来处理数据插入
        await safeDuckDBRun(connection, `
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
        await safeDuckDBRun(connection, `
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
        console.error('错误详情:', error);
      }
    }

  } catch (error) {
    console.error(`❌ 处理账户 ${accountID} 数据失败:`, error.message);
  } finally {
    connection.close();
  }
}

/**
 * 监听Firebase数据变化
 */
function setupFirebaseListener() {
  console.log('👂 开始监听Firebase数据变化...');
  
  db.ref('accounts').on('value', (snapshot) => {
    console.log('🔄 Firebase数据发生变化，开始同步...');
    const accounts = snapshot.val();
    
    Object.entries(accounts).forEach(([accountID, accountData]) => {
      if (accountData && accountData.holdings) {
        processAccountHoldings(accountID, accountData);
      }
    });
  });

  // 监听特定账户的变化
  db.ref('accounts').on('child_changed', (snapshot) => {
    const accountID = snapshot.key;
    const accountData = snapshot.val();
    console.log(`🔄 账户 ${accountID} 数据发生变化`);
    
    if (accountData && accountData.holdings) {
      processAccountHoldings(accountID, accountData);
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
 * 初始化示例数据（用于测试）
 */
async function initializeSampleData() {
  const { connection } = createDuckDBConnection();
  
  try {
    console.log('📝 开始初始化示例数据...');

    await safeDuckDBRun(connection, 'DELETE FROM tblQuotationTTM');
    await safeDuckDBRun(connection, 'DELETE FROM tblExchangeRateTTM');

    const sampleQuotations = [
      { ticker: '0006.HK', price: 28.5, currency: 'HKD' },
      { ticker: '600519.SS', price: 1600.0, currency: 'CNY' },
      { ticker: 'APO', price: 105.25, currency: 'USD' },
      { ticker: 'BAM', price: 52.75, currency: 'USD' },
      { ticker: 'BN', price: 54.25, currency: 'USD' }
    ];
    
    for (const quote of sampleQuotations) {
      await safeDuckDBRun(connection, `
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
      await safeDuckDBRun(connection, `
        INSERT INTO tblExchangeRateTTM (fromCurrency, toCurrency, rate)
        VALUES (?, ?, ?)
      `, [rate.fromCurrency, rate.toCurrency, rate.rate]);
      console.log(`✅ 添加汇率: ${rate.fromCurrency}->${rate.toCurrency} = ${rate.rate}`);
    }

    console.log('✅ 示例数据初始化完成');
  } catch (error) {
    console.error('❌ 示例数据初始化失败:', error.message);
  } finally {
    connection.close();
  }
}

/**
 * 汇总任务1: 持仓汇总计算
 */
async function createHoldingAggregationTask() {
  const { connection } = createDuckDBConnection();
  
  try {
    console.log('📈 开始执行持仓汇总任务...');
    
    // 步骤1: 按ticker汇总持仓
    const holdingSummary = await safeDuckDBQuery(connection, `
      SELECT 
        ticker,
        SUM(holding) as totalHolding,
        AVG(costPerShare) as avgCostPrice,
        SUM(holding * costPerShare) as totalCost,
        currency
      FROM tblAccountHoldings 
      WHERE ticker NOT LIKE 'CASH_%'
      GROUP BY ticker, currency
    `);

    console.log(`📊 找到 ${holdingSummary.length} 个持仓记录进行汇总`);

    if (holdingSummary.length === 0) {
      console.log('⚠️ 没有找到持仓数据，跳过汇总');
      return;
    }

    // 步骤2: 获取实时报价和汇率
    const quotations = await safeDuckDBQuery(connection, "SELECT ticker, price, currency FROM tblQuotationTTM");
    const exchangeRates = await safeDuckDBQuery(connection, "SELECT fromCurrency, toCurrency, rate FROM tblExchangeRateTTM WHERE toCurrency = 'CNY'");
    
    console.log(`📊 获取到 ${quotations.length} 个报价记录`);
    console.log(`📊 获取到 ${exchangeRates.length} 个汇率记录`);

    const quoteMap = new Map(quotations.map(q => [q.ticker, q]));
    const rateMap = new Map(exchangeRates.map(r => [`${r.fromCurrency}_${r.toCurrency}`, r.rate]));
    
    // 步骤3: 计算各项指标（使用安全转换）
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
        valueInTotal: 0
      };
    });

    // 步骤4: 计算占比
    const finalData = aggregatedData.map(item => ({
      ...item,
      costInTotal: totalCostCNY > 0 ? (item.costCNY / totalCostCNY) * 100 : 0,
      valueInTotal: totalValueCNY > 0 ? (item.valueCNY / totalValueCNY) * 100 : 0
    }));

    // 步骤5: 保存到汇总表
    await safeDuckDBRun(connection, 'DELETE FROM tblHoldingAggrView');
    
    for (const item of finalData) {
      await safeDuckDBRun(connection, `
        INSERT INTO tblHoldingAggrView 
        (ticker, totalHolding, avgCostPrice, totalCost, currentPrice, costCNY, valueCNY, PLRatio, costInTotal, valueInTotal)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        item.valueInTotal
      ]);
    }

    console.log(`✅ 持仓汇总完成，处理了 ${finalData.length} 个标的`);
    console.log(`💰 总成本: ${totalCostCNY.toFixed(2)} CNY, 总市值: ${totalValueCNY.toFixed(2)} CNY`);

    // 打印汇总结果
    console.log('\n📋 汇总结果:');
    finalData.forEach(item => {
      console.log(`   ${item.ticker}: 持仓 ${item.totalHolding}, 成本 ${item.costCNY.toFixed(2)} CNY, 市值 ${item.valueCNY.toFixed(2)} CNY, 损益 ${item.PLRatio.toFixed(2)}%`);
    });

  } catch (error) {
    console.error('❌ 持仓汇总任务失败:', error.message);
    console.error('错误堆栈:', error.stack);
  } finally {
    connection.close();
  }
}

/**
 * 检查数据库状态
 */
async function checkDatabaseStatus() {
  const { connection } = createDuckDBConnection();
  
  try {
    console.log('\n🔍 检查数据库状态...');
    
    const tableCounts = await safeDuckDBQuery(connection, `
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
    
    // 显示持仓数据 - 更新显示新字段
    const holdings = await safeDuckDBQuery(connection, 'SELECT accountID, ticker, holding, costPerShare, currency, exchangeCode, assetClass FROM tblAccountHoldings');
    console.log('\n📋 当前持仓:');
    holdings.forEach(h => {
      console.log(`  ${h.accountID} - ${h.ticker}: ${h.holding}股 @ ${h.costPerShare} ${h.currency} [${h.exchangeCode}] - ${h.assetClass}`);
    });

    // 显示报价数据
    const quotations = await safeDuckDBQuery(connection, 'SELECT ticker, price, currency FROM tblQuotationTTM');
    console.log('\n💰 当前报价:');
    quotations.forEach(q => {
      console.log(`  ${q.ticker}: ${q.price} ${q.currency}`);
    });

    // 显示汇率数据
    const rates = await safeDuckDBQuery(connection, 'SELECT fromCurrency, toCurrency, rate FROM tblExchangeRateTTM');
    console.log('\n💱 当前汇率:');
    rates.forEach(r => {
      console.log(`  ${r.fromCurrency}->${r.toCurrency}: ${r.rate}`);
    });
    
  } catch (error) {
    console.error('❌ 数据库状态检查失败:', error.message);
  } finally {
    connection.close();
  }
}

/**
 * 主函数
 */
async function main() {
  console.log('🚀 启动Firebase到DuckDB数据同步系统...');
  
  try {
    // 1. 初始化数据库（包含表结构更新）
    await initializeDatabase();
    
    // 2. 初始化示例数据
    await initializeSampleData();
    
    // 3. 首次从Firebase同步数据
    console.log('🔄 首次从Firebase同步数据...');
    const accounts = await fetchAllAccountsFromFirebase();
    for (const [accountID, accountData] of Object.entries(accounts)) {
      if (accountData && accountData.holdings) {
        await processAccountHoldings(accountID, accountData);
      }
    }
    
    // 4. 检查数据库状态
    await checkDatabaseStatus();
    
    // 5. 设置Firebase监听
    setupFirebaseListener();
    
    // 6. 注册和启动定时任务
    taskScheduler.registerTask(
      'holdingAggregation', 
      '0 0 18 * * *',
      createHoldingAggregationTask
    );
    
    taskScheduler.startAllTasks();
    
    // 7. 立即执行一次汇总任务
    setTimeout(async () => {
      await createHoldingAggregationTask();
    }, 2000);
    
    console.log('✅ 系统启动完成，开始运行...');
    
  } catch (error) {
    console.error('❌ 系统启动失败:', error.message);
  }
}

// 启动系统
main().catch(console.error);

// 优雅关闭
process.on('SIGINT', () => {
  console.log('🛑 正在关闭系统...');
  admin.app().delete().then(() => {
    console.log('✅ Firebase连接已关闭');
    process.exit(0);
  });
});

module.exports = {
  initializeDatabase,
  fetchAllAccountsFromFirebase,
  processAccountHoldings,
  createHoldingAggregationTask,
  taskScheduler
};
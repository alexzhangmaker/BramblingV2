// toolFirebase2DuckDB.js
const duckdb = require('duckdb');
const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');

// 初始化Firebase
const serviceAccount = require('/Users/zhangqing/Documents/Github/serviceKeys/bramblingV2Firebase.json');
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: 'https://outpost-8d74e-14018.firebaseio.com/'
});

const db = admin.database();
const duckDbFilePath = './PortfolioData.duckdb';

class DatabaseInitializer {
  constructor() {
    this.dbInstance = new duckdb.Database(duckDbFilePath);
  }

  createConnection() {
    const connection = this.dbInstance.connect();
    connection.run("PRAGMA threads=4");
    connection.run("PRAGMA default_order='asc'");
    connection.run("PRAGMA memory_limit='1GB'");
    return connection;
  }

  closeConnection(connection) {
    if (connection) {
      try {
        connection.close();
      } catch (error) {
        console.warn('关闭连接时出现警告:', error.message);
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

  async safeQuery(connection, query, params = []) {
    return new Promise((resolve, reject) => {
      if (params.length === 0) {
        connection.all(query, (err, result) => {
          if (err) reject(err);
          else resolve(Array.isArray(result) ? result : []);
        });
      } else {
        connection.all(query, ...params, (err, result) => {
          if (err) reject(err);
          else resolve(Array.isArray(result) ? result : []);
        });
      }
    });
  }

  /**
   * 初始化所有数据库表（从无到有）
   */
  async initializeDatabase() {
    const connection = this.createConnection();
    
    try {
      console.log('🗄️ 开始初始化数据库表结构...');

      // 删除所有现有表（如果有）
      const tables = ['tblAccountHoldings', 'tblHoldingAggrView', 'tblTaskRecords', 'tblQuotationTTM', 'tblExchangeRateTTM'];
      
      for (const table of tables) {
        try {
          await this.safeRun(connection, `DROP TABLE IF EXISTS ${table}`);
          console.log(`✅ 删除表: ${table}`);
        } catch (error) {
          console.warn(`⚠️ 删除表 ${table} 时出现警告:`, error.message);
        }
      }

      // 创建账户持仓表
      await this.safeRun(connection, `
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
      console.log('✅ 创建 tblAccountHoldings 表');

      // 创建索引
      await this.safeRun(connection, "CREATE INDEX idx_account_ticker ON tblAccountHoldings(accountID, ticker)");
      await this.safeRun(connection, "CREATE INDEX idx_ticker ON tblAccountHoldings(ticker)");

      // 创建持仓汇总表（添加 company 字段）
      await this.safeRun(connection, `
        CREATE TABLE tblHoldingAggrView (
          ticker VARCHAR PRIMARY KEY,
          company VARCHAR,
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
      console.log('✅ 创建 tblHoldingAggrView 表（包含 company 字段）');

      // 创建任务记录表
      await this.safeRun(connection, `
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
      `);
      console.log('✅ 创建 tblTaskRecords 表');

      // 创建报价表
      await this.safeRun(connection, `
        CREATE TABLE tblQuotationTTM (
          ticker VARCHAR PRIMARY KEY,
          price DOUBLE,
          currency VARCHAR,
          lastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      console.log('✅ 创建 tblQuotationTTM 表');

      // 创建汇率表
      await this.safeRun(connection, `
        CREATE TABLE tblExchangeRateTTM (
          fromCurrency VARCHAR,
          toCurrency VARCHAR,
          rate DOUBLE,
          lastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (fromCurrency, toCurrency)
        )
      `);
      console.log('✅ 创建 tblExchangeRateTTM 表');

      console.log('🎉 所有数据库表初始化完成');

    } catch (error) {
      console.error('❌ 数据库初始化失败:', error.message);
      throw error;
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 从Firebase批量同步所有账户数据
   */
  async batchSyncFromFirebase() {
    const connection = this.createConnection();
    
    try {
      console.log('🔄 开始从Firebase批量同步数据...');
      
      // 获取所有账户数据
      const snapshot = await db.ref('accounts').once('value');
      const accounts = snapshot.val() || {};
      
      console.log(`📊 从Firebase读取到 ${Object.keys(accounts).length} 个账户`);

      let totalHoldings = 0;
      let successCount = 0;

      // 开始事务
      await this.safeRun(connection, "BEGIN TRANSACTION");

      for (const [accountID, accountData] of Object.entries(accounts)) {
        if (accountData && accountData.holdings) {
          const holdings = accountData.holdings;
          const holdingsCount = Object.keys(holdings).length;
          totalHoldings += holdingsCount;

          console.log(`📦 处理账户 ${accountID} 的 ${holdingsCount} 个持仓`);

          for (const [holdingKey, holding] of Object.entries(holdings)) {
            try {
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

              successCount++;
            } catch (error) {
              console.error(`❌ 插入持仓失败 ${accountID}-${holding.ticker}:`, error.message);
            }
          }
        }
      }

      // 提交事务
      await this.safeRun(connection, "COMMIT");

      console.log(`✅ 批量同步完成: ${successCount} 个持仓记录成功插入`);
      console.log(`📈 处理了 ${Object.keys(accounts).length} 个账户，共 ${totalHoldings} 个持仓`);

      return {
        accountCount: Object.keys(accounts).length,
        totalHoldings: totalHoldings,
        successCount: successCount
      };

    } catch (error) {
      // 回滚事务
      try {
        await this.safeRun(connection, "ROLLBACK");
      } catch (rollbackError) {
        console.warn('回滚事务时出错:', rollbackError.message);
      }
      
      console.error('❌ 批量同步失败:', error.message);
      throw error;
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 初始化示例数据（可选）
   */
  async initializeSampleData() {
    const connection = this.createConnection();
    
    try {
      console.log('📝 开始初始化示例数据...');

      // 示例报价数据
      const sampleQuotations = [
        { ticker: '0006.HK', price: 28.5, currency: 'HKD' },
        { ticker: '600519.SS', price: 1600.0, currency: 'CNY' },
        { ticker: 'APO', price: 105.25, currency: 'USD' },
        { ticker: 'BAM', price: 52.75, currency: 'USD' },
        { ticker: 'US_TBill', price: 1.0, currency: 'USD' }  // 添加美国国债报价
      ];
      
      for (const quote of sampleQuotations) {
        await this.safeRun(connection, `
          INSERT INTO tblQuotationTTM (ticker, price, currency)
          VALUES (?, ?, ?)
        `, [quote.ticker, quote.price, quote.currency]);
      }

      // 示例汇率数据
      const sampleRates = [
        { fromCurrency: 'HKD', toCurrency: 'CNY', rate: 0.92 },
        { fromCurrency: 'USD', toCurrency: 'CNY', rate: 7.25 },
        { fromCurrency: 'CNY', toCurrency: 'CNY', rate: 1.0 }
      ];
      
      for (const rate of sampleRates) {
        await this.safeRun(connection, `
          INSERT INTO tblExchangeRateTTM (fromCurrency, toCurrency, rate)
          VALUES (?, ?, ?)
        `, [rate.fromCurrency, rate.toCurrency, rate.rate]);
      }

      console.log('✅ 示例数据初始化完成');

    } catch (error) {
      console.error('❌ 示例数据初始化失败:', error.message);
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 验证数据库状态
   */
  async verifyDatabase() {
    const connection = this.createConnection();
    
    try {
      console.log('🔍 验证数据库状态...');

      const tableCounts = await this.safeQuery(connection, `
        SELECT 
          (SELECT COUNT(*) FROM tblAccountHoldings) as holdings_count,
          (SELECT COUNT(*) FROM tblQuotationTTM) as quotations_count,
          (SELECT COUNT(*) FROM tblExchangeRateTTM) as rates_count
      `);

      console.log('📊 数据库统计:');
      console.log(`  持仓记录: ${tableCounts[0]?.holdings_count || 0}`);
      console.log(`  报价记录: ${tableCounts[0]?.quotations_count || 0}`);
      console.log(`  汇率记录: ${tableCounts[0]?.rates_count || 0}`);

      // 显示账户统计
      const accountStats = await this.safeQuery(connection, `
        SELECT accountID, COUNT(*) as holdings_count 
        FROM tblAccountHoldings 
        GROUP BY accountID 
        ORDER BY holdings_count DESC
      `);

      console.log('\n👤 账户持仓统计:');
      accountStats.forEach(stat => {
        console.log(`  ${stat.accountID}: ${stat.holdings_count} 个持仓`);
      });

      // 显示表结构验证
      const tableStructure = await this.safeQuery(connection, `
        PRAGMA table_info(tblHoldingAggrView)
      `);

      console.log('\n📋 tblHoldingAggrView 表结构:');
      tableStructure.forEach(column => {
        console.log(`  ${column.name} (${column.type})`);
      });

      return tableCounts[0];

    } catch (error) {
      console.error('❌ 数据库验证失败:', error.message);
    } finally {
      this.closeConnection(connection);
    }
  }
}

/**
 * 主函数
 */
async function main() {
  console.log('🚀 开始Firebase到DuckDB系统初始化...');
  
  const initializer = new DatabaseInitializer();

  try {
    // 1. 初始化数据库表结构
    await initializer.initializeDatabase();
    
    // 2. 批量同步Firebase数据
    await initializer.batchSyncFromFirebase();
    
    // 3. 初始化示例数据（可选）
    await initializer.initializeSampleData();
    
    // 4. 验证数据库状态
    await initializer.verifyDatabase();
    
    console.log('\n🎉 系统初始化完成！');
    console.log('💡 现在可以启动增量同步服务和统计任务了');

  } catch (error) {
    console.error('❌ 系统初始化失败:', error.message);
    process.exit(1);
  } finally {
    // 关闭Firebase连接
    await admin.app().delete();
  }
}

// 运行主函数
if (require.main === module) {
  main().catch(console.error);
}

module.exports = DatabaseInitializer;
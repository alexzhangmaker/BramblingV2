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
      const tables = [
        'tblAccountHoldings', 
        'tblHoldingAggrView', 
        'tblTaskRecords', 
        'tblQuotationTTM', 
        'tblExchangeRateTTM',
        'tblAccountBalanceSheet',  // 新增资产负债表
        'tblOtherAssets',  // 新增其他资产表
        'tblPeriodicBalanceSheet'  // 新增定期资产负债记录表
      ];
      
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

      // 创建账户资产负债表
      await this.safeRun(connection, `
        CREATE TABLE tblAccountBalanceSheet (
          accountID VARCHAR PRIMARY KEY,
          baseCurrency VARCHAR,
          -- 原货币计量的现金和负债
          cashOriginal DOUBLE DEFAULT 0,
          debtOriginal DOUBLE DEFAULT 0,
          -- 人民币计量的现金和负债
          cashCNY DOUBLE DEFAULT 0,
          debtCNY DOUBLE DEFAULT 0,
          -- 证券市值（人民币）
          securitiesValueCNY DOUBLE DEFAULT 0,
          -- 其他资产（人民币）
          otherAssetsCNY DOUBLE DEFAULT 0,
          -- 总净值（人民币）：现金CNY - 负债CNY + 证券市值CNY + 其他资产CNY
          totalValue DOUBLE DEFAULT 0,
          lastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      console.log('✅ 创建 tblAccountBalanceSheet 表（资产负债表）');

      // 创建其他资产表
      await this.safeRun(connection, `
        CREATE TABLE tblOtherAssets (
          assetID VARCHAR PRIMARY KEY,
          assetType VARCHAR,  -- funds, bankAccounts, insurance, properties
          accountName VARCHAR,
          currency VARCHAR,
          cost DOUBLE DEFAULT 0,
          value DOUBLE DEFAULT 0,
          deposit DOUBLE DEFAULT 0,
          loan DOUBLE DEFAULT 0,
          debt DOUBLE DEFAULT 0,
          costCNY DOUBLE DEFAULT 0,
          valueCNY DOUBLE DEFAULT 0,
          depositCNY DOUBLE DEFAULT 0,
          loanCNY DOUBLE DEFAULT 0,
          debtCNY DOUBLE DEFAULT 0,
          lastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      console.log('✅ 创建 tblOtherAssets 表（其他资产表）');

      // 创建定期资产负债记录表
      await this.safeRun(connection, `
        CREATE TABLE tblPeriodicBalanceSheet (
          periodID VARCHAR PRIMARY KEY,  -- 格式: YYYY-MM-DD
          periodDate DATE,
          -- 证券账户市值（人民币）
          securitiesValueCNY DOUBLE DEFAULT 0,
          -- 保险资产市值（人民币）
          insuranceValueCNY DOUBLE DEFAULT 0,
          -- 基金资产（人民币）
          fundsValueCNY DOUBLE DEFAULT 0,
          -- 房产资产（人民币）
          propertiesValueCNY DOUBLE DEFAULT 0,
          -- 银行存款（人民币）
          bankDepositsCNY DOUBLE DEFAULT 0,
          -- 现金总额（人民币）
          totalCashCNY DOUBLE DEFAULT 0,
          -- 负债总额（人民币）
          totalDebtCNY DOUBLE DEFAULT 0,
          -- 总资产净值（人民币）
          totalNetValueCNY DOUBLE DEFAULT 0,
          -- 详细统计
          accountCount INTEGER DEFAULT 0,
          securitiesCount INTEGER DEFAULT 0,
          insuranceCount INTEGER DEFAULT 0,
          fundsCount INTEGER DEFAULT 0,
          propertiesCount INTEGER DEFAULT 0,
          bankAccountsCount INTEGER DEFAULT 0,
          createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      console.log('✅ 创建 tblPeriodicBalanceSheet 表（定期资产负债记录表）');

      console.log('🎉 所有数据库表初始化完成');

    } catch (error) {
      console.error('❌ 数据库初始化失败:', error.message);
      throw error;
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 同步基金数据
   */
  async syncFundsData() {
    const connection = this.createConnection();
    
    try {
      console.log('📊 开始同步基金数据...');
      
      const snapshot = await db.ref('funds').once('value');
      const funds = snapshot.val() || {};
      
      console.log(`📈 从Firebase读取到 ${Object.keys(funds).length} 个基金`);

      let successCount = 0;

      await this.safeRun(connection, "BEGIN TRANSACTION");

      for (const [fundID, fundData] of Object.entries(funds)) {
        try {
          await this.safeRun(connection, `
            INSERT OR REPLACE INTO tblOtherAssets 
            (assetID, assetType, accountName, currency, cost, value)
            VALUES (?, 'funds', ?, ?, ?, ?)
          `, [fundID, fundID, fundData.currency || 'CNY', fundData.cost || 0, fundData.value || 0]);

          successCount++;
          console.log(`✅ 同步基金 ${fundID}: 成本 ${fundData.cost} ${fundData.currency}, 价值 ${fundData.value} ${fundData.currency}`);

        } catch (error) {
          console.error(`❌ 同步基金 ${fundID} 失败:`, error.message);
        }
      }

      await this.safeRun(connection, "COMMIT");
      console.log(`✅ 基金数据同步完成: ${successCount} 个基金成功同步`);

      return { successCount };

    } catch (error) {
      try { await this.safeRun(connection, "ROLLBACK"); } catch {}
      console.error('❌ 基金数据同步失败:', error.message);
      throw error;
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 同步银行账户数据
   */
  async syncBankAccountsData() {
    const connection = this.createConnection();
    
    try {
      console.log('🏦 开始同步银行账户数据...');
      
      const snapshot = await db.ref('bankAccounts').once('value');
      const bankAccounts = snapshot.val() || {};
      
      console.log(`📊 从Firebase读取到 ${Object.keys(bankAccounts).length} 个银行账户`);

      let successCount = 0;

      await this.safeRun(connection, "BEGIN TRANSACTION");

      for (const [accountID, accountData] of Object.entries(bankAccounts)) {
        try {
          await this.safeRun(connection, `
            INSERT OR REPLACE INTO tblOtherAssets 
            (assetID, assetType, accountName, currency, deposit, loan)
            VALUES (?, 'bankAccounts', ?, ?, ?, ?)
          `, [accountID, accountID, accountData.currency || 'CNY', accountData.deposit || 0, accountData.loan || 0]);

          successCount++;
          console.log(`✅ 同步银行账户 ${accountID}: 存款 ${accountData.deposit} ${accountData.currency}, 贷款 ${accountData.loan} ${accountData.currency}`);

        } catch (error) {
          console.error(`❌ 同步银行账户 ${accountID} 失败:`, error.message);
        }
      }

      await this.safeRun(connection, "COMMIT");
      console.log(`✅ 银行账户数据同步完成: ${successCount} 个账户成功同步`);

      return { successCount };

    } catch (error) {
      try { await this.safeRun(connection, "ROLLBACK"); } catch {}
      console.error('❌ 银行账户数据同步失败:', error.message);
      throw error;
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 同步保险数据
   */
  async syncInsuranceData() {
    const connection = this.createConnection();
    
    try {
      console.log('🛡️ 开始同步保险数据...');
      
      const snapshot = await db.ref('insurance').once('value');
      const insurance = snapshot.val() || {};
      
      console.log(`📊 从Firebase读取到 ${Object.keys(insurance).length} 个保险`);

      let successCount = 0;

      await this.safeRun(connection, "BEGIN TRANSACTION");

      for (const [insuranceID, insuranceData] of Object.entries(insurance)) {
        try {
          await this.safeRun(connection, `
            INSERT OR REPLACE INTO tblOtherAssets 
            (assetID, assetType, accountName, currency, cost, value)
            VALUES (?, 'insurance', ?, ?, ?, ?)
          `, [insuranceID, insuranceID, insuranceData.currency || 'CNY', insuranceData.cost || 0, insuranceData.value || 0]);

          successCount++;
          console.log(`✅ 同步保险 ${insuranceID}: 成本 ${insuranceData.cost} ${insuranceData.currency}, 价值 ${insuranceData.value} ${insuranceData.currency}`);

        } catch (error) {
          console.error(`❌ 同步保险 ${insuranceID} 失败:`, error.message);
        }
      }

      await this.safeRun(connection, "COMMIT");
      console.log(`✅ 保险数据同步完成: ${successCount} 个保险成功同步`);

      return { successCount };

    } catch (error) {
      try { await this.safeRun(connection, "ROLLBACK"); } catch {}
      console.error('❌ 保险数据同步失败:', error.message);
      throw error;
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 同步房产数据
   */
  async syncPropertiesData() {
    const connection = this.createConnection();
    
    try {
      console.log('🏠 开始同步房产数据...');
      
      const snapshot = await db.ref('properties').once('value');
      const properties = snapshot.val() || {};
      
      console.log(`📊 从Firebase读取到 ${Object.keys(properties).length} 个房产`);

      let successCount = 0;

      await this.safeRun(connection, "BEGIN TRANSACTION");

      for (const [propertyID, propertyData] of Object.entries(properties)) {
        try {
          await this.safeRun(connection, `
            INSERT OR REPLACE INTO tblOtherAssets 
            (assetID, assetType, accountName, currency, cost, value, debt)
            VALUES (?, 'properties', ?, ?, ?, ?, ?)
          `, [propertyID, propertyID, propertyData.currency || 'CNY', propertyData.cost || 0, propertyData.value || 0, propertyData.debt || 0]);

          successCount++;
          console.log(`✅ 同步房产 ${propertyID}: 成本 ${propertyData.cost} ${propertyData.currency}, 价值 ${propertyData.value} ${propertyData.currency}, 负债 ${propertyData.debt} ${propertyData.currency}`);

        } catch (error) {
          console.error(`❌ 同步房产 ${propertyID} 失败:`, error.message);
        }
      }

      await this.safeRun(connection, "COMMIT");
      console.log(`✅ 房产数据同步完成: ${successCount} 个房产成功同步`);

      return { successCount };

    } catch (error) {
      try { await this.safeRun(connection, "ROLLBACK"); } catch {}
      console.error('❌ 房产数据同步失败:', error.message);
      throw error;
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 从Firebase同步账户现金和负债数据到资产负债表
   */
  async syncAccountBalanceSheet() {
    const connection = this.createConnection();
    
    try {
      console.log('💰 开始同步账户现金和负债数据...');
      
      // 获取所有账户数据
      const snapshot = await db.ref('accounts').once('value');
      const accounts = snapshot.val() || {};
      
      console.log(`📊 从Firebase读取到 ${Object.keys(accounts).length} 个账户`);

      let successCount = 0;

      // 开始事务
      await this.safeRun(connection, "BEGIN TRANSACTION");

      for (const [accountID, accountData] of Object.entries(accounts)) {
        if (accountData) {
          try {
            const baseCurrency = accountData.meta?.currency || 'USD';
            
            // 计算现金总额（原货币）
            let cashOriginal = 0;
            if (accountData.cash) {
              for (const [currency, amount] of Object.entries(accountData.cash)) {
                cashOriginal += amount || 0;
              }
            }
            
            // 计算负债总额（原货币）
            let debtOriginal = 0;
            if (accountData.debt) {
              for (const [currency, amount] of Object.entries(accountData.debt)) {
                debtOriginal += amount || 0;
              }
            }

            // 插入或更新资产负债表数据
            await this.safeRun(connection, `
              INSERT OR REPLACE INTO tblAccountBalanceSheet 
              (accountID, baseCurrency, cashOriginal, debtOriginal, cashCNY, debtCNY, securitiesValueCNY, otherAssetsCNY, totalValue)
              VALUES (?, ?, ?, ?, 0, 0, 0, 0, 0)
            `, [accountID, baseCurrency, cashOriginal, debtOriginal]);

            successCount++;
            
            console.log(`✅ 同步账户 ${accountID}: 现金 ${cashOriginal} ${baseCurrency}, 负债 ${debtOriginal} ${baseCurrency}`);

          } catch (error) {
            console.error(`❌ 同步账户 ${accountID} 资产负债表失败:`, error.message);
          }
        }
      }

      // 提交事务
      await this.safeRun(connection, "COMMIT");

      console.log(`✅ 资产负债表同步完成: ${successCount} 个账户成功同步`);

      return {
        accountCount: Object.keys(accounts).length,
        successCount: successCount
      };

    } catch (error) {
      // 回滚事务
      try {
        await this.safeRun(connection, "ROLLBACK");
      } catch (rollbackError) {
        console.warn('回滚事务时出错:', rollbackError.message);
      }
      
      console.error('❌ 资产负债表同步失败:', error.message);
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
        { fromCurrency: 'CNY', toCurrency: 'CNY', rate: 1.0 },
        { fromCurrency: 'THB', toCurrency: 'CNY', rate: 0.20 }
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
          (SELECT COUNT(*) FROM tblExchangeRateTTM) as rates_count,
          (SELECT COUNT(*) FROM tblAccountBalanceSheet) as balance_count,
          (SELECT COUNT(*) FROM tblOtherAssets) as other_assets_count,
          (SELECT COUNT(*) FROM tblPeriodicBalanceSheet) as periodic_balance_count
      `);

      console.log('📊 数据库统计:');
      console.log(`  持仓记录: ${tableCounts[0]?.holdings_count || 0}`);
      console.log(`  报价记录: ${tableCounts[0]?.quotations_count || 0}`);
      console.log(`  汇率记录: ${tableCounts[0]?.rates_count || 0}`);
      console.log(`  资产负债表记录: ${tableCounts[0]?.balance_count || 0}`);
      console.log(`  其他资产记录: ${tableCounts[0]?.other_assets_count || 0}`);
      console.log(`  定期资产负债表记录: ${tableCounts[0]?.periodic_balance_count || 0}`);

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

      // 显示其他资产统计
      const otherAssetsStats = await this.safeQuery(connection, `
        SELECT assetType, COUNT(*) as count, 
               SUM(cost) as totalCost, SUM(value) as totalValue,
               SUM(deposit) as totalDeposit, SUM(loan) as totalLoan, SUM(debt) as totalDebt
        FROM tblOtherAssets 
        GROUP BY assetType
      `);

      console.log('\n📦 其他资产统计:');
      otherAssetsStats.forEach(stat => {
        console.log(`  ${stat.assetType}: ${stat.count} 个记录`);
        if (stat.totalCost > 0) console.log(`    总成本: ${stat.totalCost}`);
        if (stat.totalValue > 0) console.log(`    总价值: ${stat.totalValue}`);
        if (stat.totalDeposit > 0) console.log(`    总存款: ${stat.totalDeposit}`);
        if (stat.totalLoan > 0) console.log(`    总贷款: ${stat.totalLoan}`);
        if (stat.totalDebt > 0) console.log(`    总负债: ${stat.totalDebt}`);
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
    
    // 2. 批量同步Firebase持仓数据
    await initializer.batchSyncFromFirebase();
    
    // 3. 同步账户现金和负债数据到资产负债表
    await initializer.syncAccountBalanceSheet();
    
    // 4. 同步其他资产数据
    await initializer.syncFundsData();
    await initializer.syncBankAccountsData();
    await initializer.syncInsuranceData();
    await initializer.syncPropertiesData();
    
    // 5. 初始化示例数据（可选）
    await initializer.initializeSampleData();
    
    // 6. 验证数据库状态
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
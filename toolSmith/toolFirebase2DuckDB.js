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
const duckDbFilePath = path.join(__dirname, '../duckDB/PortfolioData.duckdb');

// Ensure database directory exists
const dbDir = path.dirname(duckDbFilePath);
if (!fs.existsSync(dbDir)) {
  console.log(`📁 创建数据库目录: ${dbDir}`);
  fs.mkdirSync(dbDir, { recursive: true });
}

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
        'tblAccountBalanceSheet',
        'tblOtherAssets',
        'tblPeriodicBalanceSheet'
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

      // 创建持仓汇总表
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
      console.log('✅ 创建 tblHoldingAggrView 表');

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
          cashOriginal DOUBLE DEFAULT 0,
          debtOriginal DOUBLE DEFAULT 0,
          cashCNY DOUBLE DEFAULT 0,
          debtCNY DOUBLE DEFAULT 0,
          securitiesValueCNY DOUBLE DEFAULT 0,
          otherAssetsCNY DOUBLE DEFAULT 0,
          totalValue DOUBLE DEFAULT 0,
          lastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      console.log('✅ 创建 tblAccountBalanceSheet 表');

      // 创建其他资产表
      await this.safeRun(connection, `
        CREATE TABLE tblOtherAssets (
          assetID VARCHAR PRIMARY KEY,
          assetType VARCHAR,
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
      console.log('✅ 创建 tblOtherAssets 表');

      // 创建定期资产负债记录表
      await this.safeRun(connection, `
        CREATE TABLE tblPeriodicBalanceSheet (
          periodID VARCHAR PRIMARY KEY,
          periodDate DATE,
          securitiesValueCNY DOUBLE DEFAULT 0,
          insuranceValueCNY DOUBLE DEFAULT 0,
          fundsValueCNY DOUBLE DEFAULT 0,
          propertiesValueCNY DOUBLE DEFAULT 0,
          bankDepositsCNY DOUBLE DEFAULT 0,
          totalCashCNY DOUBLE DEFAULT 0,
          totalDebtCNY DOUBLE DEFAULT 0,
          totalNetValueCNY DOUBLE DEFAULT 0,
          accountCount INTEGER DEFAULT 0,
          securitiesCount INTEGER DEFAULT 0,
          insuranceCount INTEGER DEFAULT 0,
          fundsCount INTEGER DEFAULT 0,
          propertiesCount INTEGER DEFAULT 0,
          bankAccountsCount INTEGER DEFAULT 0,
          createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      console.log('✅ 创建 tblPeriodicBalanceSheet 表');

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
    const result = {
      type: 'Funds',
      totalExpected: 0,
      successCount: 0,
      failedCount: 0,
      failedItems: []
    };

    try {
      console.log('📊 开始同步基金数据...');

      const snapshot = await db.ref('funds').once('value');
      const funds = snapshot.val() || {};
      result.totalExpected = Object.keys(funds).length;

      console.log(`📈 从Firebase读取到 ${result.totalExpected} 个基金`);

      await this.safeRun(connection, "BEGIN TRANSACTION");

      for (const [fundID, fundData] of Object.entries(funds)) {
        try {
          await this.safeRun(connection, `
            INSERT OR REPLACE INTO tblOtherAssets 
            (assetID, assetType, accountName, currency, cost, value)
            VALUES (?, 'funds', ?, ?, ?, ?)
          `, [fundID, fundID, fundData.currency || 'CNY', fundData.cost || 0, fundData.value || 0]);

          result.successCount++;
        } catch (error) {
          result.failedCount++;
          result.failedItems.push({ id: fundID, error: error.message });
          console.error(`❌ 同步基金 ${fundID} 失败:`, error.message);
        }
      }

      await this.safeRun(connection, "COMMIT");
      console.log(`✅ 基金数据同步完成: ${result.successCount}/${result.totalExpected}`);

      return result;

    } catch (error) {
      try { await this.safeRun(connection, "ROLLBACK"); } catch { }
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
    const result = {
      type: 'BankAccounts',
      totalExpected: 0,
      successCount: 0,
      failedCount: 0,
      failedItems: []
    };

    try {
      console.log('🏦 开始同步银行账户数据...');

      const snapshot = await db.ref('bankAccounts').once('value');
      const bankAccounts = snapshot.val() || {};
      result.totalExpected = Object.keys(bankAccounts).length;

      console.log(`📊 从Firebase读取到 ${result.totalExpected} 个银行账户`);

      await this.safeRun(connection, "BEGIN TRANSACTION");

      for (const [accountID, accountData] of Object.entries(bankAccounts)) {
        try {
          await this.safeRun(connection, `
            INSERT OR REPLACE INTO tblOtherAssets 
            (assetID, assetType, accountName, currency, deposit, loan)
            VALUES (?, 'bankAccounts', ?, ?, ?, ?)
          `, [accountID, accountID, accountData.currency || 'CNY', accountData.deposit || 0, accountData.loan || 0]);

          result.successCount++;
        } catch (error) {
          result.failedCount++;
          result.failedItems.push({ id: accountID, error: error.message });
          console.error(`❌ 同步银行账户 ${accountID} 失败:`, error.message);
        }
      }

      await this.safeRun(connection, "COMMIT");
      console.log(`✅ 银行账户数据同步完成: ${result.successCount}/${result.totalExpected}`);

      return result;

    } catch (error) {
      try { await this.safeRun(connection, "ROLLBACK"); } catch { }
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
    const result = {
      type: 'Insurance',
      totalExpected: 0,
      successCount: 0,
      failedCount: 0,
      failedItems: []
    };

    try {
      console.log('🛡️ 开始同步保险数据...');

      const snapshot = await db.ref('insurance').once('value');
      const insurance = snapshot.val() || {};
      result.totalExpected = Object.keys(insurance).length;

      console.log(`📊 从Firebase读取到 ${result.totalExpected} 个保险`);

      await this.safeRun(connection, "BEGIN TRANSACTION");

      for (const [insuranceID, insuranceData] of Object.entries(insurance)) {
        try {
          await this.safeRun(connection, `
            INSERT OR REPLACE INTO tblOtherAssets 
            (assetID, assetType, accountName, currency, cost, value)
            VALUES (?, 'insurance', ?, ?, ?, ?)
          `, [insuranceID, insuranceID, insuranceData.currency || 'CNY', insuranceData.cost || 0, insuranceData.value || 0]);

          result.successCount++;
        } catch (error) {
          result.failedCount++;
          result.failedItems.push({ id: insuranceID, error: error.message });
          console.error(`❌ 同步保险 ${insuranceID} 失败:`, error.message);
        }
      }

      await this.safeRun(connection, "COMMIT");
      console.log(`✅ 保险数据同步完成: ${result.successCount}/${result.totalExpected}`);

      return result;

    } catch (error) {
      try { await this.safeRun(connection, "ROLLBACK"); } catch { }
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
    const result = {
      type: 'Properties',
      totalExpected: 0,
      successCount: 0,
      failedCount: 0,
      failedItems: []
    };

    try {
      console.log('🏠 开始同步房产数据...');

      const snapshot = await db.ref('properties').once('value');
      const properties = snapshot.val() || {};
      result.totalExpected = Object.keys(properties).length;

      console.log(`📊 从Firebase读取到 ${result.totalExpected} 个房产`);

      await this.safeRun(connection, "BEGIN TRANSACTION");

      for (const [propertyID, propertyData] of Object.entries(properties)) {
        try {
          await this.safeRun(connection, `
            INSERT OR REPLACE INTO tblOtherAssets 
            (assetID, assetType, accountName, currency, cost, value, debt)
            VALUES (?, 'properties', ?, ?, ?, ?, ?)
          `, [propertyID, propertyID, propertyData.currency || 'CNY', propertyData.cost || 0, propertyData.value || 0, propertyData.debt || 0]);

          result.successCount++;
        } catch (error) {
          result.failedCount++;
          result.failedItems.push({ id: propertyID, error: error.message });
          console.error(`❌ 同步房产 ${propertyID} 失败:`, error.message);
        }
      }

      await this.safeRun(connection, "COMMIT");
      console.log(`✅ 房产数据同步完成: ${result.successCount}/${result.totalExpected}`);

      return result;

    } catch (error) {
      try { await this.safeRun(connection, "ROLLBACK"); } catch { }
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
    const result = {
      type: 'AccountBalanceSheet',
      totalExpected: 0, // Accounts count
      successCount: 0,
      failedCount: 0,
      failedItems: []
    };

    try {
      console.log('💰 开始同步账户现金和负债数据...');

      // 获取所有账户数据
      const snapshot = await db.ref('accounts').once('value');
      const accounts = snapshot.val() || {};
      result.totalExpected = Object.keys(accounts).length;

      console.log(`📊 从Firebase读取到 ${result.totalExpected} 个账户`);

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

            result.successCount++;

          } catch (error) {
            result.failedCount++;
            result.failedItems.push({ id: accountID, error: error.message });
            console.error(`❌ 同步账户 ${accountID} 资产负债表失败:`, error.message);
          }
        }
      }

      // 提交事务
      await this.safeRun(connection, "COMMIT");

      console.log(`✅ 资产负债表同步完成: ${result.successCount}/${result.totalExpected}`);

      return result;

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
    const result = {
      type: 'AccountHoldings',
      totalAccountsExpected: 0,
      totalHoldingsExpected: 0,
      successCount: 0,
      failedCount: 0,
      failedItems: [] // { id: "account-ticker", error: msg }
    };

    try {
      console.log('🔄 开始从Firebase批量同步数据...');

      // 获取所有账户数据
      const snapshot = await db.ref('accounts').once('value');
      const accounts = snapshot.val() || {};
      result.totalAccountsExpected = Object.keys(accounts).length;

      console.log(`📊 从Firebase读取到 ${result.totalAccountsExpected} 个账户`);

      // 开始事务
      await this.safeRun(connection, "BEGIN TRANSACTION");

      for (const [accountID, accountData] of Object.entries(accounts)) {
        if (accountData && accountData.holdings) {
          const holdings = accountData.holdings;
          const holdingsCount = Object.keys(holdings).length;
          result.totalHoldingsExpected += holdingsCount;

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

              result.successCount++;
            } catch (error) {
              result.failedCount++;
              result.failedItems.push({ id: `${accountID}-${holding.ticker}`, error: error.message });
              console.error(`❌ 插入持仓失败 ${accountID}-${holding.ticker}:`, error.message);
            }
          }
        }
      }

      // 提交事务
      await this.safeRun(connection, "COMMIT");

      console.log(`✅ 批量同步完成: ${result.successCount} 个持仓记录成功插入`);
      console.log(`📈 处理了 ${result.totalAccountsExpected} 个账户，共 ${result.totalHoldingsExpected} 个持仓`);

      return result;

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
        { ticker: 'US_TBill', price: 1.0, currency: 'USD' }
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
   * 验证数据库状态，并对比预期值
   */
  async verifyDatabase(fullReport) {
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

      // Helper function to handle BigInt conversion safely
      const getCount = (val) => {
        if (typeof val === 'bigint') return Number(val);
        return Number(val) || 0;
      };

      const counts = {
        holdings_count: getCount(tableCounts[0].holdings_count),
        quotations_count: getCount(tableCounts[0].quotations_count),
        rates_count: getCount(tableCounts[0].rates_count),
        balance_count: getCount(tableCounts[0].balance_count),
        other_assets_count: getCount(tableCounts[0].other_assets_count),
        periodic_balance_count: getCount(tableCounts[0].periodic_balance_count)
      };

      // 验证持仓数量
      const holdingsResult = fullReport.find(r => r.type === 'AccountHoldings');
      if (holdingsResult) {
        if (counts.holdings_count !== holdingsResult.successCount) {
          console.error(`⚠️ 持仓数量不匹配! DuckDB: ${counts.holdings_count}, 成功插入: ${holdingsResult.successCount}`);
          holdingsResult.validationError = `DB count (${counts.holdings_count}) != Success count (${holdingsResult.successCount})`;
        } else {
          console.log(`✅ 持仓数量验证通过 (${counts.holdings_count})`);
        }
      }

      // 验证资产负债表账户数
      const balanceResult = fullReport.find(r => r.type === 'AccountBalanceSheet');
      if (balanceResult) {
        if (counts.balance_count !== balanceResult.successCount) {
          console.error(`⚠️ 资产负债表账户数不匹配! DuckDB: ${counts.balance_count}, 成功插入: ${balanceResult.successCount}`);
          balanceResult.validationError = `DB count (${counts.balance_count}) != Success count (${balanceResult.successCount})`;
        } else {
          console.log(`✅ 资产负债表账户数验证通过 (${counts.balance_count})`);
        }
      }

      // 验证其他资产总数 (Funds + Bank + Insurance + Properties)
      const otherAssetsTotalExpected =
        (fullReport.find(r => r.type === 'Funds')?.successCount || 0) +
        (fullReport.find(r => r.type === 'BankAccounts')?.successCount || 0) +
        (fullReport.find(r => r.type === 'Insurance')?.successCount || 0) +
        (fullReport.find(r => r.type === 'Properties')?.successCount || 0);

      if (counts.other_assets_count !== otherAssetsTotalExpected) {
        console.error(`⚠️ 其他资产总数不匹配! DuckDB: ${counts.other_assets_count}, 预期: ${otherAssetsTotalExpected}`);
      } else {
        console.log(`✅ 其他资产总数验证通过 (${counts.other_assets_count})`);
      }

    } catch (error) {
      console.error('❌ 数据库验证失败:', error.message);
    } finally {
      this.closeConnection(connection);
    }
  }
}

/**
 * 打印同步报告
 */
function printSyncReport(reports) {
  console.log('\n==================================================');
  console.log('📊 同步结果报告 (SYNC REPORT)');
  console.log('==================================================');

  let hasErrors = false;

  reports.forEach(r => {
    if (!r) return;

    // 计算成功率
    const total = r.totalExpected || r.totalHoldingsExpected || 0;
    const rate = total > 0 ? ((r.successCount / total) * 100).toFixed(1) + '%' : 'N/A';

    // 状态图标
    let statusIcon = '✅';
    if (r.failedCount > 0) statusIcon = '⚠️';
    if (r.validationError) statusIcon = '❌';

    console.log(`${statusIcon} [${r.type}]`);
    console.log(`   总数: ${total} | 成功: ${r.successCount} | 失败: ${r.failedCount} | 成功率: ${rate}`);

    if (r.validationError) {
      console.log(`   🛑 验证错误: ${r.validationError}`);
      hasErrors = true;
    }

    if (r.failedCount > 0) {
      hasErrors = true;
      console.log(`   🔴 失败项详情:`);
      if (r.failedItems.length > 10) {
        r.failedItems.slice(0, 10).forEach(item => console.log(`      - ID: ${item.id}, Err: ${item.error}`));
        console.log(`      ... 以及其他 ${r.failedItems.length - 10} 项`);
      } else {
        r.failedItems.forEach(item => console.log(`      - ID: ${item.id}, Err: ${item.error}`));
      }
    }
    console.log('--------------------------------------------------');
  });

  console.log('\n==================================================');
  if (hasErrors) {
    console.log('❌ 同步完成，但存在错误或警告，请检查上方日志。');
  } else {
    console.log('✅ 同步完美完成，数据完整性校验通过。');
  }
  console.log('==================================================\n');
}

/**
 * 主函数
 */
async function main() {
  console.log('🚀 开始Firebase到DuckDB系统初始化...');

  const initializer = new DatabaseInitializer();
  const fullReport = [];

  try {
    // 1. 初始化数据库表结构
    await initializer.initializeDatabase();

    // 2. 批量同步Firebase持仓数据
    const holdingsReport = await initializer.batchSyncFromFirebase();
    fullReport.push(holdingsReport);

    // 3. 同步账户现金和负债数据到资产负债表
    const balanceReport = await initializer.syncAccountBalanceSheet();
    fullReport.push(balanceReport);

    // 4. 同步其他资产数据
    fullReport.push(await initializer.syncFundsData());
    fullReport.push(await initializer.syncBankAccountsData());
    fullReport.push(await initializer.syncInsuranceData());
    fullReport.push(await initializer.syncPropertiesData());

    // 5. 初始化示例数据（可选）
    await initializer.initializeSampleData();

    // 6. 验证数据库状态
    await initializer.verifyDatabase(fullReport);

    // 7. 打印最终报告
    printSyncReport(fullReport);

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
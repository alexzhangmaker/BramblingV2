// createPeriodicBalanceSheetTable.js
const duckdb = require('duckdb');

const duckDbFilePath = './PortfolioData.duckdb';

class TableCreator {
  constructor() {
    this.dbInstance = new duckdb.Database(duckDbFilePath);
  }

  createConnection() {
    const connection = this.dbInstance.connect();
    connection.run("PRAGMA threads=4");
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
   * 创建定期资产负债记录表
   */
  async createPeriodicBalanceSheetTable() {
    const connection = this.createConnection();
    
    try {
      console.log('🗄️ 开始创建定期资产负债记录表...');

      // 检查表是否已存在
      const tableExists = await this.safeQuery(connection, `
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='tblPeriodicBalanceSheet'
      `);

      if (tableExists.length > 0) {
        console.log('ℹ️ 表 tblPeriodicBalanceSheet 已存在，先删除...');
        await this.safeRun(connection, `DROP TABLE IF EXISTS tblPeriodicBalanceSheet`);
        console.log('✅ 删除旧表完成');
      }

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

      // 验证表结构
      const tableStructure = await this.safeQuery(connection, `
        PRAGMA table_info(tblPeriodicBalanceSheet)
      `);

      console.log('\n📋 tblPeriodicBalanceSheet 表结构:');
      tableStructure.forEach(column => {
        console.log(`  ${column.name} (${column.type}) ${column.notnull ? 'NOT NULL' : ''} ${column.pk ? 'PRIMARY KEY' : ''}`);
      });

      console.log('\n🎉 定期资产负债记录表创建完成！');

    } catch (error) {
      console.error('❌ 创建表失败:', error.message);
      throw error;
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 验证数据库连接和状态
   */
  async verifyDatabase() {
    const connection = this.createConnection();
    
    try {
      console.log('🔍 验证数据库状态...');

      // 检查所有表
      const tables = await this.safeQuery(connection, `
        SELECT name FROM sqlite_master 
        WHERE type='table' 
        ORDER BY name
      `);

      console.log('📊 数据库中的表:');
      tables.forEach(table => {
        console.log(`  - ${table.name}`);
      });

      // 检查定期资产负债表记录
      const periodicCount = await this.safeQuery(connection, `
        SELECT COUNT(*) as count FROM tblPeriodicBalanceSheet
      `);

      console.log(`\n📈 定期资产负债表记录数: ${periodicCount[0]?.count || 0}`);

      return tables;

    } catch (error) {
      console.error('❌ 数据库验证失败:', error.message);
      return [];
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 插入测试数据（可选）
   */
  async insertTestData() {
    const connection = this.createConnection();
    
    try {
      console.log('📝 插入测试数据...');

      const testData = [
        {
          periodID: '2024-01-01',
          periodDate: '2024-01-01',
          securitiesValueCNY: 1000000,
          insuranceValueCNY: 500000,
          fundsValueCNY: 300000,
          propertiesValueCNY: 2000000,
          bankDepositsCNY: 500000,
          totalCashCNY: 100000,
          totalDebtCNY: 800000,
          totalNetValueCNY: 3600000,
          accountCount: 3,
          securitiesCount: 15,
          insuranceCount: 5,
          fundsCount: 3,
          propertiesCount: 2,
          bankAccountsCount: 4
        },
        {
          periodID: '2024-01-02',
          periodDate: '2024-01-02',
          securitiesValueCNY: 1010000,
          insuranceValueCNY: 500000,
          fundsValueCNY: 300000,
          propertiesValueCNY: 2000000,
          bankDepositsCNY: 500000,
          totalCashCNY: 100000,
          totalDebtCNY: 800000,
          totalNetValueCNY: 3610000,
          accountCount: 3,
          securitiesCount: 15,
          insuranceCount: 5,
          fundsCount: 3,
          propertiesCount: 2,
          bankAccountsCount: 4
        }
      ];

      for (const data of testData) {
        await this.safeRun(connection, `
          INSERT OR REPLACE INTO tblPeriodicBalanceSheet 
          (periodID, periodDate, securitiesValueCNY, insuranceValueCNY, fundsValueCNY, 
           propertiesValueCNY, bankDepositsCNY, totalCashCNY, totalDebtCNY, totalNetValueCNY,
           accountCount, securitiesCount, insuranceCount, fundsCount, propertiesCount, bankAccountsCount)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `, [
          data.periodID,
          data.periodDate,
          data.securitiesValueCNY,
          data.insuranceValueCNY,
          data.fundsValueCNY,
          data.propertiesValueCNY,
          data.bankDepositsCNY,
          data.totalCashCNY,
          data.totalDebtCNY,
          data.totalNetValueCNY,
          data.accountCount,
          data.securitiesCount,
          data.insuranceCount,
          data.fundsCount,
          data.propertiesCount,
          data.bankAccountsCount
        ]);
      }

      console.log('✅ 测试数据插入完成');

    } catch (error) {
      console.error('❌ 插入测试数据失败:', error.message);
    } finally {
      this.closeConnection(connection);
    }
  }
}

/**
 * 主函数
 */
async function main() {
  console.log('🚀 开始创建定期资产负债记录表...');
  
  const tableCreator = new TableCreator();

  try {
    // 创建表
    await tableCreator.createPeriodicBalanceSheetTable();

    // 如果指定了测试数据
    if (process.argv.includes('--test-data')) {
      await tableCreator.insertTestData();
    }

    // 验证数据库状态
    await tableCreator.verifyDatabase();

    console.log('\n🎉 表创建完成！');
    console.log('💡 现在可以测试 svcPeriodicalBalanceSheetAll.js 了');

  } catch (error) {
    console.error('❌ 表创建失败:', error.message);
    process.exit(1);
  }
}

// 运行主函数
if (require.main === module) {
  main().catch(console.error);
}

module.exports = TableCreator;
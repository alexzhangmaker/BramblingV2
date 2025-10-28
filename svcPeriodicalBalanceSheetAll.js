// svcPeriodicalBalanceSheetAll.js
const duckdb = require('duckdb');
const nodeCron = require('node-cron');

const duckDbFilePath = './PortfolioData.duckdb';

class PeriodicalBalanceSheetService {
  constructor() {
    this.dbInstance = new duckdb.Database(duckDbFilePath);
  }

  createConnection() {
    const connection = this.dbInstance.connect();
    connection.run("PRAGMA threads=4");
    connection.run("PRAGMA memory_limit='2GB'");
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
   * 更新其他资产的CNY价值（使用汇率转换）
   */
  async updateOtherAssetsCNYValue() {
    const connection = this.createConnection();
    
    try {
      console.log('💰 开始更新其他资产的人民币价值...');
      
      await this.safeRun(connection, "BEGIN TRANSACTION");

      // 更新其他资产的CNY价值
      const updateQuery = `
        UPDATE tblOtherAssets 
        SET 
          costCNY = CASE 
            WHEN currency = 'CNY' THEN cost 
            ELSE cost * COALESCE((SELECT rate FROM tblExchangeRateTTM WHERE fromCurrency = tblOtherAssets.currency AND toCurrency = 'CNY'), 1)
          END,
          valueCNY = CASE 
            WHEN currency = 'CNY' THEN value 
            ELSE value * COALESCE((SELECT rate FROM tblExchangeRateTTM WHERE fromCurrency = tblOtherAssets.currency AND toCurrency = 'CNY'), 1)
          END,
          depositCNY = CASE 
            WHEN currency = 'CNY' THEN deposit 
            ELSE deposit * COALESCE((SELECT rate FROM tblExchangeRateTTM WHERE fromCurrency = tblOtherAssets.currency AND toCurrency = 'CNY'), 1)
          END,
          loanCNY = CASE 
            WHEN currency = 'CNY' THEN loan 
            ELSE loan * COALESCE((SELECT rate FROM tblExchangeRateTTM WHERE fromCurrency = tblOtherAssets.currency AND toCurrency = 'CNY'), 1)
          END,
          debtCNY = CASE 
            WHEN currency = 'CNY' THEN debt 
            ELSE debt * COALESCE((SELECT rate FROM tblExchangeRateTTM WHERE fromCurrency = tblOtherAssets.currency AND toCurrency = 'CNY'), 1)
          END,
          lastUpdated = CURRENT_TIMESTAMP
        WHERE currency IS NOT NULL
      `;

      const result = await this.safeRun(connection, updateQuery);
      
      // 获取更新统计
      const stats = await this.safeQuery(connection, `
        SELECT 
          assetType,
          COUNT(*) as count,
          SUM(costCNY) as totalCostCNY,
          SUM(valueCNY) as totalValueCNY,
          SUM(depositCNY) as totalDepositCNY,
          SUM(loanCNY) as totalLoanCNY,
          SUM(debtCNY) as totalDebtCNY
        FROM tblOtherAssets 
        GROUP BY assetType
      `);

      await this.safeRun(connection, "COMMIT");

      console.log('✅ 其他资产人民币价值更新完成');
      console.log('📊 其他资产统计:');
      stats.forEach(stat => {
        console.log(`  ${stat.assetType}: ${stat.count} 个记录`);
        if (stat.totalCostCNY > 0) console.log(`    总成本: ${stat.totalCostCNY.toFixed(2)} CNY`);
        if (stat.totalValueCNY > 0) console.log(`    总价值: ${stat.totalValueCNY.toFixed(2)} CNY`);
        if (stat.totalDepositCNY > 0) console.log(`    总存款: ${stat.totalDepositCNY.toFixed(2)} CNY`);
        if (stat.totalLoanCNY > 0) console.log(`    总贷款: ${stat.totalLoanCNY.toFixed(2)} CNY`);
        if (stat.totalDebtCNY > 0) console.log(`    总负债: ${stat.totalDebtCNY.toFixed(2)} CNY`);
      });

      return stats;

    } catch (error) {
      try {
        await this.safeRun(connection, "ROLLBACK");
      } catch (rollbackError) {
        console.warn('回滚事务时出错:', rollbackError.message);
      }
      
      console.error('❌ 更新其他资产人民币价值失败:', error.message);
      throw error;
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 更新账户资产负债表的CNY价值
   */
  async updateAccountBalanceSheetCNYValue() {
    const connection = this.createConnection();
    
    try {
      console.log('💰 开始更新账户资产负债表的人民币价值...');
      
      await this.safeRun(connection, "BEGIN TRANSACTION");

      // 更新账户资产负债表的CNY价值
      const updateQuery = `
        UPDATE tblAccountBalanceSheet 
        SET 
          cashCNY = CASE 
            WHEN baseCurrency = 'CNY' THEN cashOriginal 
            ELSE cashOriginal * COALESCE((SELECT rate FROM tblExchangeRateTTM WHERE fromCurrency = tblAccountBalanceSheet.baseCurrency AND toCurrency = 'CNY'), 1)
          END,
          debtCNY = CASE 
            WHEN baseCurrency = 'CNY' THEN debtOriginal 
            ELSE debtOriginal * COALESCE((SELECT rate FROM tblExchangeRateTTM WHERE fromCurrency = tblAccountBalanceSheet.baseCurrency AND toCurrency = 'CNY'), 1)
          END,
          lastUpdated = CURRENT_TIMESTAMP
        WHERE baseCurrency IS NOT NULL
      `;

      await this.safeRun(connection, updateQuery);
      
      // 获取更新统计
      const stats = await this.safeQuery(connection, `
        SELECT 
          COUNT(*) as accountCount,
          SUM(cashCNY) as totalCashCNY,
          SUM(debtCNY) as totalDebtCNY
        FROM tblAccountBalanceSheet
      `);

      await this.safeRun(connection, "COMMIT");

      console.log('✅ 账户资产负债表人民币价值更新完成');
      console.log(`📊 账户统计: ${stats[0]?.accountCount || 0} 个账户`);
      console.log(`   总现金: ${stats[0]?.totalCashCNY?.toFixed(2) || 0} CNY`);
      console.log(`   总负债: ${stats[0]?.totalDebtCNY?.toFixed(2) || 0} CNY`);

      return stats[0];

    } catch (error) {
      try {
        await this.safeRun(connection, "ROLLBACK");
      } catch (rollbackError) {
        console.warn('回滚事务时出错:', rollbackError.message);
      }
      
      console.error('❌ 更新账户资产负债表人民币价值失败:', error.message);
      throw error;
    } finally {
      this.closeConnection(connection);
    }
  }


  /**
   * 计算并记录定期资产负债表
   */
  async calculatePeriodicBalanceSheet() {
    const connection = this.createConnection();
    
    try {
      console.log('📊 开始计算定期资产负债表...');
      
      // 先更新所有CNY价值
      await this.updateOtherAssetsCNYValue();
      await this.updateAccountBalanceSheetCNYValue();

      const currentDate = new Date().toISOString().split('T')[0]; // YYYY-MM-DD
      const periodID = currentDate;

      await this.safeRun(connection, "BEGIN TRANSACTION");

      // 计算各项资产的人民币价值
      const balanceSheetData = await this.safeQuery(connection, `
        WITH 
        -- 证券账户市值
        securities_total AS (
          SELECT 
            SUM(valueCNY) as securitiesValueCNY,
            COUNT(*) as securitiesCount
          FROM tblHoldingAggrView
          WHERE valueCNY > 0
        ),
        -- 保险资产（使用valueCNY）
        insurance_total AS (
          SELECT 
            SUM(valueCNY) as insuranceValueCNY,
            COUNT(*) as insuranceCount
          FROM tblOtherAssets 
          WHERE assetType = 'insurance' AND valueCNY > 0
        ),
        -- 基金资产（使用valueCNY）
        funds_total AS (
          SELECT 
            SUM(valueCNY) as fundsValueCNY,
            COUNT(*) as fundsCount
          FROM tblOtherAssets 
          WHERE assetType = 'funds' AND valueCNY > 0
        ),
        -- 房产资产（净值 = valueCNY - debtCNY）
        properties_total AS (
          SELECT 
            SUM(valueCNY - debtCNY) as propertiesValueCNY,
            COUNT(*) as propertiesCount
          FROM tblOtherAssets 
          WHERE assetType = 'properties' AND (valueCNY > 0 OR debtCNY > 0)
        ),
        -- 银行存款（净值 = depositCNY - loanCNY）
        bank_deposits_total AS (
          SELECT 
            SUM(depositCNY - loanCNY) as bankDepositsCNY,
            COUNT(*) as bankAccountsCount
          FROM tblOtherAssets 
          WHERE assetType = 'bankAccounts' AND (depositCNY > 0 OR loanCNY > 0)
        ),
        -- 现金和负债总额
        cash_debt_total AS (
          SELECT 
            SUM(cashCNY) as totalCashCNY,
            SUM(debtCNY) as totalDebtCNY,
            COUNT(*) as accountCount
          FROM tblAccountBalanceSheet
        )
        SELECT 
          -- 资产项目
          COALESCE(s.securitiesValueCNY, 0) as securitiesValueCNY,
          COALESCE(i.insuranceValueCNY, 0) as insuranceValueCNY,
          COALESCE(f.fundsValueCNY, 0) as fundsValueCNY,
          COALESCE(p.propertiesValueCNY, 0) as propertiesValueCNY,
          COALESCE(b.bankDepositsCNY, 0) as bankDepositsCNY,
          COALESCE(c.totalCashCNY, 0) as totalCashCNY,
          -- 负债项目
          COALESCE(c.totalDebtCNY, 0) as totalDebtCNY,
          -- 统计数量
          COALESCE(c.accountCount, 0) as accountCount,
          COALESCE(s.securitiesCount, 0) as securitiesCount,
          COALESCE(i.insuranceCount, 0) as insuranceCount,
          COALESCE(f.fundsCount, 0) as fundsCount,
          COALESCE(p.propertiesCount, 0) as propertiesCount,
          COALESCE(b.bankAccountsCount, 0) as bankAccountsCount
        FROM securities_total s
        CROSS JOIN insurance_total i
        CROSS JOIN funds_total f
        CROSS JOIN properties_total p
        CROSS JOIN bank_deposits_total b
        CROSS JOIN cash_debt_total c
      `);

      if (balanceSheetData.length === 0) {
        throw new Error('无法计算资产负债表数据');
      }

      const data = balanceSheetData[0];
      
      // 计算总资产净值
      const totalNetValueCNY = 
        data.securitiesValueCNY +
        data.insuranceValueCNY +
        data.fundsValueCNY +
        data.propertiesValueCNY +
        data.bankDepositsCNY +
        data.totalCashCNY -
        data.totalDebtCNY;

      // 插入定期资产负债表记录
      await this.safeRun(connection, `
        INSERT OR REPLACE INTO tblPeriodicBalanceSheet 
        (periodID, periodDate, securitiesValueCNY, insuranceValueCNY, fundsValueCNY, 
         propertiesValueCNY, bankDepositsCNY, totalCashCNY, totalDebtCNY, totalNetValueCNY,
         accountCount, securitiesCount, insuranceCount, fundsCount, propertiesCount, bankAccountsCount)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        periodID,
        currentDate,
        data.securitiesValueCNY,
        data.insuranceValueCNY,
        data.fundsValueCNY,
        data.propertiesValueCNY,
        data.bankDepositsCNY,
        data.totalCashCNY,
        data.totalDebtCNY,
        totalNetValueCNY,
        data.accountCount,
        data.securitiesCount,
        data.insuranceCount,
        data.fundsCount,
        data.propertiesCount,
        data.bankAccountsCount
      ]);

      await this.safeRun(connection, "COMMIT");

      console.log('✅ 定期资产负债表计算完成');
      console.log(`📈 ${currentDate} 资产负债表统计:`);
      console.log(`   证券账户市值: ${data.securitiesValueCNY.toFixed(2)} CNY (${data.securitiesCount} 个标的)`);
      console.log(`   保险资产: ${data.insuranceValueCNY.toFixed(2)} CNY (${data.insuranceCount} 个保险)`);
      console.log(`   基金资产: ${data.fundsValueCNY.toFixed(2)} CNY (${data.fundsCount} 个基金)`);
      console.log(`   房产资产: ${data.propertiesValueCNY.toFixed(2)} CNY (${data.propertiesCount} 个房产)`);
      console.log(`   银行存款: ${data.bankDepositsCNY.toFixed(2)} CNY (${data.bankAccountsCount} 个账户)`);
      console.log(`   现金总额: ${data.totalCashCNY.toFixed(2)} CNY`);
      console.log(`   负债总额: ${data.totalDebtCNY.toFixed(2)} CNY`);
      console.log(`   总资产净值: ${totalNetValueCNY.toFixed(2)} CNY`);

      // 显示资产构成比例
      const totalAssets = data.securitiesValueCNY + data.insuranceValueCNY + data.fundsValueCNY + 
                         data.propertiesValueCNY + data.bankDepositsCNY + data.totalCashCNY;
      
      if (totalAssets > 0) {
        console.log('\n📊 资产构成比例:');
        console.log(`   证券账户: ${((data.securitiesValueCNY / totalAssets) * 100).toFixed(2)}%`);
        console.log(`   保险资产: ${((data.insuranceValueCNY / totalAssets) * 100).toFixed(2)}%`);
        console.log(`   基金资产: ${((data.fundsValueCNY / totalAssets) * 100).toFixed(2)}%`);
        console.log(`   房产资产: ${((data.propertiesValueCNY / totalAssets) * 100).toFixed(2)}%`);
        console.log(`   银行存款: ${((data.bankDepositsCNY / totalAssets) * 100).toFixed(2)}%`);
        console.log(`   现金: ${((data.totalCashCNY / totalAssets) * 100).toFixed(2)}%`);
      }

      return {
        periodID,
        totalNetValueCNY,
        ...data
      };

    } catch (error) {
      try {
        await this.safeRun(connection, "ROLLBACK");
      } catch (rollbackError) {
        console.warn('回滚事务时出错:', rollbackError.message);
      }
      
      console.error('❌ 定期资产负债表计算失败:', error.message);
      throw error;
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 调试函数：显示其他资产详细信息
   */
  async debugOtherAssets() {
    const connection = this.createConnection();
    
    try {
      console.log('🔍 调试其他资产数据...');

      const assets = await this.safeQuery(connection, `
        SELECT 
          assetID,
          assetType,
          accountName,
          currency,
          cost,
          value,
          deposit,
          loan,
          debt,
          costCNY,
          valueCNY,
          depositCNY,
          loanCNY,
          debtCNY
        FROM tblOtherAssets 
        ORDER BY assetType, assetID
      `);

      console.log(`📊 找到 ${assets.length} 个其他资产记录:`);
      
      assets.forEach(asset => {
        console.log(`\n  ${asset.assetType} - ${asset.assetID}:`);
        console.log(`    货币: ${asset.currency}`);
        if (asset.cost > 0) console.log(`    成本: ${asset.cost} (${asset.costCNY} CNY)`);
        if (asset.value > 0) console.log(`    价值: ${asset.value} (${asset.valueCNY} CNY)`);
        if (asset.deposit > 0) console.log(`    存款: ${asset.deposit} (${asset.depositCNY} CNY)`);
        if (asset.loan > 0) console.log(`    贷款: ${asset.loan} (${asset.loanCNY} CNY)`);
        if (asset.debt > 0) console.log(`    负债: ${asset.debt} (${asset.debtCNY} CNY)`);
      });

      return assets;

    } catch (error) {
      console.error('❌ 调试其他资产数据失败:', error.message);
      return [];
    } finally {
      this.closeConnection(connection);
    }
  }



  /**
   * 获取历史资产负债表数据
   */
  async getHistoricalBalanceSheet(days = 30) {
    const connection = this.createConnection();
    
    try {
      console.log(`📈 获取最近 ${days} 天资产负债表历史数据...`);

      const history = await this.safeQuery(connection, `
        SELECT 
          periodDate,
          securitiesValueCNY,
          insuranceValueCNY,
          fundsValueCNY,
          propertiesValueCNY,
          bankDepositsCNY,
          totalCashCNY,
          totalDebtCNY,
          totalNetValueCNY,
          accountCount,
          securitiesCount,
          insuranceCount,
          fundsCount,
          propertiesCount,
          bankAccountsCount
        FROM tblPeriodicBalanceSheet 
        WHERE periodDate >= date('now', ? || ' days')
        ORDER BY periodDate DESC
      `, [`-${days}`]);

      console.log(`📊 找到 ${history.length} 条历史记录`);

      // 显示历史数据摘要
      if (history.length > 0) {
        console.log('\n📅 历史数据摘要:');
        const latest = history[0];
        const oldest = history[history.length - 1];
        
        const netValueChange = latest.totalNetValueCNY - oldest.totalNetValueCNY;
        const changePercentage = oldest.totalNetValueCNY > 0 ? (netValueChange / oldest.totalNetValueCNY) * 100 : 0;
        
        console.log(`   最新净值: ${latest.totalNetValueCNY.toFixed(2)} CNY (${latest.periodDate})`);
        console.log(`   最早净值: ${oldest.totalNetValueCNY.toFixed(2)} CNY (${oldest.periodDate})`);
        console.log(`   期间变化: ${netValueChange.toFixed(2)} CNY (${changePercentage.toFixed(2)}%)`);
      }

      return history;

    } catch (error) {
      console.error('❌ 获取历史资产负债表数据失败:', error.message);
      return [];
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 生成资产负债表报告
   */
  async generateBalanceSheetReport(startDate, endDate) {
    const connection = this.createConnection();
    
    try {
      console.log(`📋 生成资产负债表报告 ${startDate} 至 ${endDate}...`);

      const report = await this.safeQuery(connection, `
        SELECT 
          periodDate,
          securitiesValueCNY,
          insuranceValueCNY,
          fundsValueCNY,
          propertiesValueCNY,
          bankDepositsCNY,
          totalCashCNY,
          totalDebtCNY,
          totalNetValueCNY,
          accountCount
        FROM tblPeriodicBalanceSheet 
        WHERE periodDate BETWEEN ? AND ?
        ORDER BY periodDate ASC
      `, [startDate, endDate]);

      if (report.length === 0) {
        console.log('ℹ️ 指定时间段内无数据');
        return [];
      }

      console.log(`📊 生成报告包含 ${report.length} 条记录`);

      // 计算统计信息
      const firstRecord = report[0];
      const lastRecord = report[report.length - 1];
      
      const netValueChange = lastRecord.totalNetValueCNY - firstRecord.totalNetValueCNY;
      const changePercentage = firstRecord.totalNetValueCNY > 0 ? 
        (netValueChange / firstRecord.totalNetValueCNY) * 100 : 0;

      console.log('\n📈 报告统计:');
      console.log(`   起始日期: ${firstRecord.periodDate}`);
      console.log(`   结束日期: ${lastRecord.periodDate}`);
      console.log(`   起始净值: ${firstRecord.totalNetValueCNY.toFixed(2)} CNY`);
      console.log(`   结束净值: ${lastRecord.totalNetValueCNY.toFixed(2)} CNY`);
      console.log(`   净值变化: ${netValueChange.toFixed(2)} CNY (${changePercentage.toFixed(2)}%)`);
      console.log(`   日均变化: ${(netValueChange / report.length).toFixed(2)} CNY`);

      return report;

    } catch (error) {
      console.error('❌ 生成资产负债表报告失败:', error.message);
      return [];
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 启动定期资产负债表计算任务
   */
  startPeriodicBalanceSheetTask(cronExpression = '0 0 18 * * *') { // 默认每天18:00执行
    console.log(`⏰ 启动定期资产负债表任务，计划: ${cronExpression}`);
    
    nodeCron.schedule(cronExpression, async () => {
      console.log('🚀 定时执行资产负债表计算任务...');
      try {
        await this.calculatePeriodicBalanceSheet();
        console.log('✅ 定时资产负债表任务完成');
      } catch (error) {
        console.error('❌ 定时资产负债表任务失败:', error.message);
      }
    });
    
    console.log('✅ 定期资产负债表任务已启动');
  }

  /**
   * 立即执行一次资产负债表计算
   */
  async executeBalanceSheetImmediately() {
    try {
      console.log('⚡ 立即执行资产负债表计算...');
      await this.calculatePeriodicBalanceSheet();
      console.log('✅ 立即执行完成');
    } catch (error) {
      console.error('❌ 立即执行资产负债表计算失败:', error.message);
      throw error;
    }
  }

  /**
   * 验证资产负债表数据
   */
  async validateBalanceSheetData() {
    const connection = this.createConnection();
    
    try {
      console.log('🔍 验证资产负债表数据...');

      // 检查各表数据完整性
      const tableStats = await this.safeQuery(connection, `
        SELECT 
          (SELECT COUNT(*) FROM tblHoldingAggrView) as holding_aggr_count,
          (SELECT COUNT(*) FROM tblAccountBalanceSheet) as balance_sheet_count,
          (SELECT COUNT(*) FROM tblOtherAssets) as other_assets_count,
          (SELECT COUNT(*) FROM tblQuotationTTM) as quotation_count,
          (SELECT COUNT(*) FROM tblExchangeRateTTM) as exchange_rate_count
      `);

      const stats = tableStats[0];
      console.log('📊 数据完整性检查:');
      console.log(`   持仓汇总记录: ${stats.holding_aggr_count}`);
      console.log(`   资产负债表记录: ${stats.balance_sheet_count}`);
      console.log(`   其他资产记录: ${stats.other_assets_count}`);
      console.log(`   报价记录: ${stats.quotation_count}`);
      console.log(`   汇率记录: ${stats.exchange_rate_count}`);

      // 检查是否有缺失的汇率数据
      const missingRates = await this.safeQuery(connection, `
        SELECT DISTINCT currency 
        FROM (
          SELECT currency FROM tblAccountHoldings
          UNION 
          SELECT currency FROM tblOtherAssets
          UNION
          SELECT currency FROM tblQuotationTTM
        ) 
        WHERE currency NOT IN (SELECT fromCurrency FROM tblExchangeRateTTM WHERE toCurrency = 'CNY')
          AND currency != 'CNY'
      `);

      if (missingRates.length > 0) {
        console.log('⚠️  缺少以下货币的汇率数据:');
        missingRates.forEach(rate => {
          console.log(`   - ${rate.currency}`);
        });
      } else {
        console.log('✅ 汇率数据完整');
      }

      return stats;

    } catch (error) {
      console.error('❌ 资产负债表数据验证失败:', error.message);
      return null;
    } finally {
      this.closeConnection(connection);
    }
  }
}

/**
 * 主函数
 */
async function main() {
  console.log('🚀 启动定期资产负债表服务...');
  
  const balanceSheetService = new PeriodicalBalanceSheetService();

  // 注册关闭信号
  process.on('SIGINT', () => {
    console.log('🛑 停止定期资产负债表服务...');
    process.exit(0);
  });

  try {
    // 命令行参数处理
    if (process.argv.includes('--debug-assets')) {
      console.log('🔍 调试其他资产数据...');
      await balanceSheetService.debugOtherAssets();
    } else if (process.argv.includes('--update-cny')) {
      console.log('💰 更新CNY价值...');
      await balanceSheetService.updateOtherAssetsCNYValue();
      await balanceSheetService.updateAccountBalanceSheetCNYValue();
    } else if (process.argv.includes('--immediate')) {
      console.log('⚡ 立即执行资产负债表计算...');
      await balanceSheetService.executeBalanceSheetImmediately();
    } else if (process.argv.includes('--history')) {
      const days = process.argv[process.argv.indexOf('--history') + 1] || 30;
      await balanceSheetService.getHistoricalBalanceSheet(parseInt(days));
    } else if (process.argv.includes('--report')) {
      const startDate = process.argv[process.argv.indexOf('--report') + 1];
      const endDate = process.argv[process.argv.indexOf('--report') + 2];
      if (startDate && endDate) {
        await balanceSheetService.generateBalanceSheetReport(startDate, endDate);
      } else {
        console.log('❌ 请提供开始日期和结束日期: --report YYYY-MM-DD YYYY-MM-DD');
      }
    } else if (process.argv.includes('--validate')) {
      await balanceSheetService.validateBalanceSheetData();
    } else {
      // 默认启动定时任务
      console.log('⏰ 启动定时资产负债表计算任务...');
      balanceSheetService.startPeriodicBalanceSheetTask('0 0 18 * * *'); // 每天18:00执行
      
      // 立即执行一次
      console.log('⚡ 立即执行一次资产负债表计算...');
      await balanceSheetService.executeBalanceSheetImmediately();
      
      console.log('✅ 定期资产负债表服务运行中...');
      console.log('💡 使用 Ctrl+C 停止服务');
      
      // 保持进程运行
      setInterval(() => {
        // 心跳检测，保持进程活跃
      }, 60000);
    }
    
  } catch (error) {
    console.error('❌ 定期资产负债表服务启动失败:', error.message);
    process.exit(1);
  }
}

// 运行主函数
if (require.main === module) {
  main().catch(console.error);
}

module.exports = PeriodicalBalanceSheetService;
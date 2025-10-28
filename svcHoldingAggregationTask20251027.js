// holdingAggregationTask.js
const duckdb = require('duckdb');
const nodeCron = require('node-cron');

const duckDbFilePath = './portfolioData.duckdb';

class HoldingAggregationTask {
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
   * 使用DuckDB窗口函数进行高效汇总计算
   */
  async executeAggregation() {
    const connection = this.createConnection();
    
    try {
      console.log('📈 开始执行持仓汇总计算...');
      
      // 开始事务
      await this.safeRun(connection, "BEGIN TRANSACTION");

      // 步骤1: 清空汇总表
      await this.safeRun(connection, "DELETE FROM tblHoldingAggrView");

      // 步骤2: 使用窗口函数一次性计算所有汇总指标
      const aggregationQuery = `
        WITH holding_totals AS (
          -- 按ticker汇总基础数据
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
        ),
        quotes AS (
          -- 获取最新报价
          SELECT ticker, price, currency 
          FROM tblQuotationTTM
        ),
        rates AS (
          -- 获取汇率
          SELECT fromCurrency, toCurrency, rate 
          FROM tblExchangeRateTTM 
          WHERE toCurrency = 'CNY'
        ),
        converted_holdings AS (
          -- 转换为CNY
          SELECT 
            ht.ticker,
            ht.totalHolding,
            ht.avgCostPrice,
            ht.totalCost,
            ht.currency,
            ht.accountCount,
            COALESCE(q.price, 0) as currentPrice,
            COALESCE(r.rate, 1) as exchangeRate,
            ht.totalCost * COALESCE(r.rate, 1) as costCNY,
            ht.totalHolding * COALESCE(q.price, 0) * COALESCE(r.rate, 1) as valueCNY
          FROM holding_totals ht
          LEFT JOIN quotes q ON ht.ticker = q.ticker
          LEFT JOIN rates r ON ht.currency = r.fromCurrency
        ),
        totals AS (
          -- 计算总计
          SELECT 
            SUM(costCNY) as totalCostCNY,
            SUM(valueCNY) as totalValueCNY
          FROM converted_holdings
        )
        -- 最终插入，使用窗口函数计算百分比
        INSERT INTO tblHoldingAggrView 
        (ticker, totalHolding, avgCostPrice, totalCost, currentPrice, costCNY, valueCNY, PLRatio, costInTotal, valueInTotal, accountCount, currency)
        SELECT 
          ch.ticker,
          ch.totalHolding,
          ch.avgCostPrice,
          ch.totalCost,
          ch.currentPrice,
          ch.costCNY,
          ch.valueCNY,
          CASE 
            WHEN ch.costCNY > 0 THEN ((ch.valueCNY - ch.costCNY) / ch.costCNY) * 100 
            ELSE 0 
          END as PLRatio,
          CASE 
            WHEN t.totalCostCNY > 0 THEN (ch.costCNY / t.totalCostCNY) * 100 
            ELSE 0 
          END as costInTotal,
          CASE 
            WHEN t.totalValueCNY > 0 THEN (ch.valueCNY / t.totalValueCNY) * 100 
            ELSE 0 
          END as valueInTotal,
          ch.accountCount,
          ch.currency
        FROM converted_holdings ch
        CROSS JOIN totals t
        ORDER BY ch.valueCNY DESC
      `;

      await this.safeRun(connection, aggregationQuery);

      // 提交事务
      await this.safeRun(connection, "COMMIT");

      // 获取统计结果
      const stats = await this.safeQuery(connection, `
        SELECT 
          COUNT(*) as totalTickers,
          SUM(totalHolding) as totalShares,
          SUM(costCNY) as totalCostCNY,
          SUM(valueCNY) as totalValueCNY,
          AVG(PLRatio) as avgPLRatio
        FROM tblHoldingAggrView
      `);

      console.log('✅ 持仓汇总计算完成');
      console.log(`📊 汇总统计:`);
      console.log(`   标的数量: ${stats[0]?.totalTickers || 0}`);
      console.log(`   总股数: ${stats[0]?.totalShares || 0}`);
      console.log(`   总成本: ${(stats[0]?.totalCostCNY || 0).toFixed(2)} CNY`);
      console.log(`   总市值: ${(stats[0]?.totalValueCNY || 0).toFixed(2)} CNY`);
      console.log(`   平均损益: ${(stats[0]?.avgPLRatio || 0).toFixed(2)}%`);

      return stats[0];

    } catch (error) {
      // 回滚事务
      try {
        await this.safeRun(connection, "ROLLBACK");
      } catch (rollbackError) {
        console.warn('回滚事务时出错:', rollbackError.message);
      }
      
      console.error('❌ 持仓汇总计算失败:', error.message);
      throw error;
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 启动定时任务
   */
  startScheduledTask(cronExpression = '0 0 18 * * *') { // 默认每天18:00执行
    console.log(`⏰ 启动定时汇总任务，计划: ${cronExpression}`);
    
    nodeCron.schedule(cronExpression, async () => {
      console.log('🚀 定时执行持仓汇总任务...');
      try {
        await this.executeAggregation();
        console.log('✅ 定时汇总任务完成');
      } catch (error) {
        console.error('❌ 定时汇总任务失败:', error.message);
      }
    });
    
    console.log('✅ 定时任务已启动');
  }

  /**
   * 立即执行一次汇总任务
   */
  async executeImmediately() {
    try {
      await this.executeAggregation();
    } catch (error) {
      console.error('❌ 立即执行汇总任务失败:', error.message);
      throw error;
    }
  }
}

/**
 * 主函数
 */
async function main() {
  console.log('🚀 启动持仓汇总任务服务...');
  
  const aggregationTask = new HoldingAggregationTask();
  
  // 注册关闭信号
  process.on('SIGINT', () => {
    console.log('🛑 停止汇总任务服务...');
    process.exit(0);
  });

  try {
    // 启动定时任务（每天18:00执行）
    aggregationTask.startScheduledTask('0 0 18 * * *');
    
    // 立即执行一次（可选）
    if (process.argv.includes('--immediate')) {
      console.log('⚡ 立即执行汇总任务...');
      await aggregationTask.executeImmediately();
    }
    
    console.log('✅ 汇总任务服务运行中...');
    console.log('💡 使用 Ctrl+C 停止服务');
    
    // 保持进程运行
    setInterval(() => {
      // 心跳检测，保持进程活跃
    }, 60000);
    
  } catch (error) {
    console.error('❌ 汇总任务服务启动失败:', error.message);
    process.exit(1);
  }
}

// 运行主函数
if (require.main === module) {
  main().catch(console.error);
}

module.exports = HoldingAggregationTask;
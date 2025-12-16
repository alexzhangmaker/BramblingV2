// holdingAggregationTask.js
const duckdb = require('duckdb');
const nodeCron = require('node-cron');

const path = require('path');

const duckDbFilePath = path.join(__dirname, 'duckDB/PortfolioData.duckdb');

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
   * 判断是否为美国国债
   */
  isUSTreasury(ticker, assetClass, description) {
    if (!ticker) return false;

    const desc = (description || '').toLowerCase();
    const asset = (assetClass || '').toLowerCase();

    // 美国国债的判断条件
    return asset === 'bond' ||
      asset === 'govt' ||
      desc.includes('treasury') ||
      desc.includes('t-bill') ||
      desc.includes('t bill') ||
      desc.includes('government bond') ||
      desc.includes('govt bond') ||
      desc.includes('ust') ||
      ticker === 'US_TBill' ||
      ticker.includes('TF Float') ||
      ticker.includes('Treasury');
  }

  /**
   * 预处理持仓数据，合并美国国债
   */
  async preprocessHoldings(connection) {
    try {
      console.log('🔧 预处理持仓数据，合并美国国债...');

      // 获取所有美国国债持仓
      const usTreasuryHoldings = await this.safeQuery(connection, `
        SELECT 
          accountID,
          ticker,
          company,
          costPerShare,
          currency,
          holding,
          assetClass,
          description
        FROM tblAccountHoldings 
        WHERE assetClass = 'BOND' 
           OR assetClass = 'Govt'
           OR description LIKE '%Treasury%'
           OR description LIKE '%T-Bill%'
           OR ticker = 'US_TBill'
           OR ticker LIKE 'TF Float%'
      `);

      if (usTreasuryHoldings.length === 0) {
        console.log('ℹ️ 未找到美国国债持仓');
        return;
      }

      console.log(`📊 找到 ${usTreasuryHoldings.length} 个美国国债持仓记录`);

      // 按账户分组显示美国国债
      const accountMap = new Map();
      usTreasuryHoldings.forEach(holding => {
        if (!accountMap.has(holding.accountID)) {
          accountMap.set(holding.accountID, []);
        }
        accountMap.get(holding.accountID).push(holding);
      });

      console.log('\n🇺🇸 美国国债分布:');
      accountMap.forEach((holdings, accountID) => {
        console.log(`  ${accountID}: ${holdings.length} 个国债持仓`);
        holdings.forEach(h => {
          console.log(`    - ${h.ticker}: ${h.holding}股 @ ${h.costPerShare} ${h.currency}`);
        });
      });

    } catch (error) {
      console.warn('⚠️ 预处理持仓数据失败:', error.message);
    }
  }


  /**
 * 使用DuckDB窗口函数进行高效汇总计算（美国国债使用面值成本）
 */
  /*
  async executeAggregation() {
    const connection = this.createConnection();
    
    try {
      console.log('📈 开始执行持仓汇总计算...');
      
      // 开始事务
      await this.safeRun(connection, "BEGIN TRANSACTION");
  
      // 步骤1: 清空汇总表
      await this.safeRun(connection, "DELETE FROM tblHoldingAggrView");
  
      // 步骤2: 使用窗口函数一次性计算所有汇总指标（美国国债使用面值成本）
      const aggregationQuery = `
        WITH normalized_holdings AS (
          -- 标准化持仓数据，处理美国国债
          SELECT 
            accountID,
            CASE 
              -- 识别并合并美国国债
              WHEN assetClass IN ('BOND', 'Govt') OR 
                   description LIKE '%Treasury%' OR 
                   description LIKE '%T-Bill%' OR
                   ticker = 'US_TBill' OR
                   ticker LIKE 'TF Float%'
              THEN 'US_TBill'
              ELSE ticker 
            END as normalized_ticker,
            company,
            costPerShare,  -- 保留原始成本用于其他证券
            currency,
            holding,
            exchange,
            exchangeCode,
            assetClass,
            description
          FROM tblAccountHoldings 
          WHERE ticker NOT LIKE 'CASH_%'
        ),
        holding_totals AS (
          -- 按标准化ticker汇总基础数据
          SELECT 
            normalized_ticker as ticker,
            SUM(holding) as totalHolding,
            -- 平均成本价计算：美国国债使用1.0，其他使用加权平均
            CASE 
              WHEN normalized_ticker = 'US_TBill' THEN 1.0  -- 美国国债面值成本为1.0 USD
              ELSE SUM(holding * costPerShare) / NULLIF(SUM(holding), 0)
            END as avgCostPrice,
            -- 总成本计算：美国国债使用 totalHolding × 1.0，其他使用加权成本
            CASE 
              WHEN normalized_ticker = 'US_TBill' THEN SUM(holding) * 1.0  -- 美国国债总成本 = 面值 × 1.0
              ELSE SUM(holding * costPerShare)
            END as totalCost,
            currency,
            COUNT(DISTINCT accountID) as accountCount,
            -- 记录原始ticker信息（用于美国国债）
            CASE 
              WHEN normalized_ticker = 'US_TBill' THEN 
                'US Treasury Bills Aggregate'
              ELSE 
                MAX(company) 
            END as company_name
          FROM normalized_holdings 
          GROUP BY normalized_ticker, currency
          HAVING SUM(holding) > 0  -- 只包含有实际持仓的记录
        ),
        quotes AS (
          -- 获取最新报价（为美国国债设置面值价格1.0）
          SELECT 
            ticker, 
            CASE 
              WHEN ticker = 'US_TBill' THEN 1.0  -- 美国国债面值价格为1.0 USD
              ELSE COALESCE(price, 0)
            END as price, 
            currency 
          FROM tblQuotationTTM
          UNION ALL
          -- 为没有报价的美国国债添加面值价格
          SELECT 
            'US_TBill' as ticker,
            1.0 as price,  -- 美国国债面值价格为1.0 USD
            'USD' as currency
          WHERE NOT EXISTS (SELECT 1 FROM tblQuotationTTM WHERE ticker = 'US_TBill')
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
            ht.company_name,
            COALESCE(q.price, 0) as currentPrice,
            COALESCE(r.rate, 1) as exchangeRate,
            -- 成本CNY：美国国债成本就是 totalHolding × 1.0 × 汇率
            ht.totalCost * COALESCE(r.rate, 1) as costCNY,
            -- 市值CNY：美国国债市值就是 totalHolding × 1.0 × 汇率
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
  
      // 显示美国国债的汇总情况
      await this.showUSTreasurySummary(connection);
  
      return stats[0];
  
    } catch (error) {
      // 回滚事务
      try {
        await this.safeRun(connection, "ROLLBACK");
      } catch (rollbackError) {
        console.warn('回滚事务时出错:', rollbackError.message);
      }
      
      console.error('❌ 持仓汇总计算失败:', error.message);
      console.error('错误详情:', error);
      throw error;
    } finally {
      this.closeConnection(connection);
    }
  }
  */
  /**
   * 使用DuckDB窗口函数进行高效汇总计算（美国国债使用面值成本）
   */
  async executeAggregation() {
    const connection = this.createConnection();

    try {
      console.log('📈 开始执行持仓汇总计算...');

      // 开始事务
      await this.safeRun(connection, "BEGIN TRANSACTION");

      // 步骤1: 清空汇总表
      await this.safeRun(connection, "DELETE FROM tblHoldingAggrView");

      // 步骤2: 使用窗口函数一次性计算所有汇总指标（美国国债使用面值成本）
      const aggregationQuery = `
      WITH normalized_holdings AS (
        -- 标准化持仓数据，处理美国国债
        SELECT 
          accountID,
          CASE 
            -- 识别并合并美国国债
            WHEN assetClass IN ('BOND', 'Govt') OR 
                 description LIKE '%Treasury%' OR 
                 description LIKE '%T-Bill%' OR
                 ticker = 'US_TBill' OR
                 ticker LIKE 'TF Float%'
            THEN 'US_TBill'
            ELSE ticker 
          END as normalized_ticker,
          company,
          costPerShare,  -- 保留原始成本用于其他证券
          currency,
          holding,
          exchange,
          exchangeCode,
          assetClass,
          description
        FROM tblAccountHoldings 
        WHERE ticker NOT LIKE 'CASH_%'
      ),
      holding_totals AS (
        -- 按标准化ticker汇总基础数据
        SELECT 
          normalized_ticker as ticker,
          SUM(holding) as totalHolding,
          -- 平均成本价计算：美国国债使用1.0，其他使用加权平均
          CASE 
            WHEN normalized_ticker = 'US_TBill' THEN 1.0  -- 美国国债面值成本为1.0 USD
            ELSE SUM(holding * costPerShare) / NULLIF(SUM(holding), 0)
          END as avgCostPrice,
          -- 总成本计算：美国国债使用 totalHolding × 1.0，其他使用加权成本
          CASE 
            WHEN normalized_ticker = 'US_TBill' THEN SUM(holding) * 1.0  -- 美国国债总成本 = 面值 × 1.0
            ELSE SUM(holding * costPerShare)
          END as totalCost,
          currency,
          COUNT(DISTINCT accountID) as accountCount,
          -- 记录公司名称：美国国债使用特定名称，其他使用最常见的公司名称
          CASE 
            WHEN normalized_ticker = 'US_TBill' THEN 
              'US Treasury Bills Aggregate'
            ELSE 
              MAX(company) 
          END as company
        FROM normalized_holdings 
        GROUP BY normalized_ticker, currency
        HAVING SUM(holding) > 0  -- 只包含有实际持仓的记录
      ),
      quotes AS (
        -- 获取最新报价（为美国国债设置面值价格1.0）
        SELECT 
          ticker, 
          CASE 
            WHEN ticker = 'US_TBill' THEN 1.0  -- 美国国债面值价格为1.0 USD
            ELSE COALESCE(price, 0)
          END as price, 
          currency 
        FROM tblQuotationTTM
        UNION ALL
        -- 为没有报价的美国国债添加面值价格
        SELECT 
          'US_TBill' as ticker,
          1.0 as price,  -- 美国国债面值价格为1.0 USD
          'USD' as currency
        WHERE NOT EXISTS (SELECT 1 FROM tblQuotationTTM WHERE ticker = 'US_TBill')
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
          ht.company,
          COALESCE(q.price, 0) as currentPrice,
          COALESCE(r.rate, 1) as exchangeRate,
          -- 成本CNY：美国国债成本就是 totalHolding × 1.0 × 汇率
          ht.totalCost * COALESCE(r.rate, 1) as costCNY,
          -- 市值CNY：美国国债市值就是 totalHolding × 1.0 × 汇率
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
      (ticker, totalHolding, avgCostPrice, totalCost, currentPrice, costCNY, valueCNY, PLRatio, costInTotal, valueInTotal, accountCount, currency, company)
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
        ch.currency,
        ch.company
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

      // 显示美国国债的汇总情况
      await this.showUSTreasurySummary(connection);

      return stats[0];

    } catch (error) {
      // 回滚事务
      try {
        await this.safeRun(connection, "ROLLBACK");
      } catch (rollbackError) {
        console.warn('回滚事务时出错:', rollbackError.message);
      }

      console.error('❌ 持仓汇总计算失败:', error.message);
      console.error('错误详情:', error);
      throw error;
    } finally {
      this.closeConnection(connection);
    }
  }


  /**
   * 显示美国国债汇总情况
   */
  async showUSTreasurySummary(connection) {
    try {
      const usTreasuryStats = await this.safeQuery(connection, `
        SELECT 
          ticker,
          totalHolding,
          avgCostPrice,
          totalCost,
          costCNY,
          valueCNY,
          PLRatio,
          accountCount
        FROM tblHoldingAggrView 
        WHERE ticker = 'US_TBill'
      `);

      if (usTreasuryStats.length > 0) {
        const treasury = usTreasuryStats[0];
        console.log('\n🇺🇸 美国国债汇总:');
        console.log(`   总面值: ${treasury.totalHolding?.toLocaleString() || '0'} USD`);
        console.log(`   平均成本价格: ${treasury.avgCostPrice?.toFixed(4) || 'N/A'} USD`);
        console.log(`   总成本: ${treasury.totalCost?.toLocaleString() || 'N/A'} USD`);
        console.log(`   成本(CNY): ${treasury.costCNY?.toFixed(2) || 'N/A'} CNY`);
        console.log(`   当前市值: ${treasury.valueCNY?.toFixed(2) || 'N/A'} CNY`);
        console.log(`   涉及账户: ${treasury.accountCount} 个`);
        console.log(`   当前损益: ${treasury.PLRatio?.toFixed(2) || 'N/A'}%`);

        // 解释美国国债的计算逻辑
        console.log(`\n💡 美国国债计算说明:`);
        console.log(`   - 持仓数量: 记录的是面值金额 (如 40,000 = 40,000 USD面值)`);
        console.log(`   - 成本价格: 保留券商原始数据 (如 100.04415 = 100.04415% 面值)`);
        console.log(`   - 当前价格: 使用面值价格 100.0 (100% 面值)`);
        console.log(`   - 市值计算: (面值金额 / 100) × 100.0 × 汇率`);
        console.log(`   - 损益计算: 反映债券价格相对于面值的波动`);

        // 计算美国国债在总投资中的占比
        const totalStats = await this.safeQuery(connection, `
          SELECT 
            SUM(costCNY) as totalCostCNY,
            SUM(valueCNY) as totalValueCNY
          FROM tblHoldingAggrView
        `);

        const totalCostCNY = totalStats[0]?.totalCostCNY || 0;
        const totalValueCNY = totalStats[0]?.totalValueCNY || 0;

        if (totalCostCNY > 0) {
          const costPercentage = (treasury.costCNY / totalCostCNY) * 100;
          console.log(`   成本占比: ${costPercentage.toFixed(2)}%`);
        }
        if (totalValueCNY > 0) {
          const valuePercentage = (treasury.valueCNY / totalValueCNY) * 100;
          console.log(`   市值占比: ${valuePercentage.toFixed(2)}%`);
        }
      } else {
        console.log('\nℹ️ 未找到美国国债汇总数据');
      }

    } catch (error) {
      console.warn('⚠️ 获取美国国债汇总失败:', error.message);
    }
  }

  /**
   * 强制更新美国国债报价为100.0（面值价格）
   */
  async fixUSTreasuryQuotes() {
    const connection = this.createConnection();

    try {
      console.log('🔧 强制更新美国国债报价为100.0（面值价格）...');

      await this.safeRun(connection, "BEGIN TRANSACTION");

      // 更新或插入美国国债报价
      await this.safeRun(connection, `
        INSERT OR REPLACE INTO tblQuotationTTM (ticker, price, currency, lastUpdated)
        VALUES ('US_TBill', 100.0, 'USD', CURRENT_TIMESTAMP)
      `);

      await this.safeRun(connection, "COMMIT");

      console.log('✅ 美国国债报价已更新为100.0 USD（面值价格）');

    } catch (error) {
      try {
        await this.safeRun(connection, "ROLLBACK");
      } catch (rollbackError) {
        console.warn('回滚事务时出错:', rollbackError.message);
      }
      console.error('❌ 更新美国国债报价失败:', error.message);
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 验证美国国债数据一致性
   */
  async validateUSTreasuryData() {
    const connection = this.createConnection();

    try {
      console.log('🔍 验证美国国债数据一致性...');

      // 检查原始持仓数据中的美国国债
      const originalHoldings = await this.safeQuery(connection, `
        SELECT 
          accountID,
          ticker,
          costPerShare,
          holding,
          currency,
          assetClass,
          description
        FROM tblAccountHoldings 
        WHERE assetClass IN ('BOND', 'Govt') 
           OR description LIKE '%Treasury%' 
           OR description LIKE '%T-Bill%'
           OR ticker = 'US_TBill'
           OR ticker LIKE 'TF Float%'
      `);

      if (originalHoldings.length > 0) {
        console.log(`📊 找到 ${originalHoldings.length} 个美国国债原始持仓记录:`);

        originalHoldings.forEach(holding => {
          console.log(`   ${holding.accountID} - ${holding.ticker}: ${holding.holding} @ ${holding.costPerShare} ${holding.currency}`);

          // 检查成本价格是否接近1.0
          if (Math.abs(holding.costPerShare - 1.0) > 0.01) {
            console.log(`   ⚠️  注意: ${holding.ticker} 的成本价格 ${holding.costPerShare} 与1.0有差异`);
          }
        });
      } else {
        console.log('ℹ️ 未找到美国国债原始持仓记录');
      }

    } catch (error) {
      console.warn('⚠️ 验证美国国债数据失败:', error.message);
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
 * 主函数（添加验证选项）
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
    // 如果指定了验证美国国债数据
    if (process.argv.includes('--validate-treasury')) {
      console.log('🔍 执行美国国债数据验证...');
      await aggregationTask.validateUSTreasuryData();
      process.exit(0);
    }

    // 启动定时任务（每天18:00执行）
    aggregationTask.startScheduledTask('0 0 18 * * *');

    // 立即执行一次（可选）
    if (process.argv.includes('--immediate')) {
      console.log('⚡ 立即执行汇总任务...');
      await aggregationTask.executeImmediately();
      console.log('✅ 立即执行完成，退出进程');
      process.exit(0); // 立即执行完成后退出
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
// svcUpdateExchangeRate.js
const duckdb = require('duckdb');
const nodeCron = require('node-cron');
const APIModuleYahoo = require("./API_YFinance");

const path = require('path');

const duckDbFilePath = path.join(__dirname, 'duckDB/PortfolioData.duckdb');

class ExchangeRateUpdateService {
  constructor() {
    this.dbInstance = new duckdb.Database(duckDbFilePath);
    this.isUpdating = false;
    // 支持的货币对：所有货币都转换为CNY
    this.supportedCurrencies = ['USD', 'HKD', 'GBP', 'CAD', 'EUR', 'JPY', 'AUD', 'SGD', 'CHF'];
    this.baseCurrency = 'CNY';
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
   * 模拟汇率API调用函数
   * 在实际使用中，这里应该替换为真实的API调用
   */
  async API_FetchExRate(from, to) {
    let retValue = await await APIModuleYahoo.API_FetchExRate(from, to);
    return retValue;
    /*
    // 模拟API调用延迟
    await new Promise(resolve => setTimeout(resolve, 200 + Math.random() * 300));
    
    // 模拟汇率数据（基于真实汇率的近似值）
    const exchangeRates = {
      'USD_CNY': 7.25,
      'HKD_CNY': 0.92,
      'GBP_CNY': 9.15,
      'CAD_CNY': 5.35,
      'EUR_CNY': 7.85,
      'JPY_CNY': 0.049,
      'AUD_CNY': 4.75,
      'SGD_CNY': 5.40,
      'CHF_CNY': 8.05,
      'CNY_CNY': 1.00
    };
    
    const rateKey = `${from}_${to}`;
    let rate = exchangeRates[rateKey];
    
    // 如果直接汇率不存在，尝试反向计算
    if (!rate) {
      const reverseKey = `${to}_${from}`;
      const reverseRate = exchangeRates[reverseKey];
      if (reverseRate) {
        rate = 1 / reverseRate;
      }
    }
    
    // 如果还是没有找到，使用默认值并添加小幅随机波动 (±1%)
    if (!rate) {
      // 基于货币的基准汇率估算
      const baseRates = {
        'USD': 7.25, 'HKD': 0.92, 'GBP': 9.15, 'CAD': 5.35,
        'EUR': 7.85, 'JPY': 0.049, 'AUD': 4.75, 'SGD': 5.40,
        'CHF': 8.05, 'CNY': 1.00
      };
      
      const fromRate = baseRates[from] || 1;
      const toRate = baseRates[to] || 1;
      rate = toRate / fromRate;
      
      // 添加小幅随机波动 (±1%)
      const fluctuation = (Math.random() - 0.5) * 0.02;
      rate = rate * (1 + fluctuation);
    }
    
    console.log(`💱 获取汇率 ${from} -> ${to}: ${rate.toFixed(4)}`);
    
    return parseFloat(rate.toFixed(6));
    */
  }

  /**
   * 更新汇率数据
   */
  async updateExchangeRate() {
    if (this.isUpdating) {
      console.log('⚠️ 汇率更新正在进行中，跳过本次执行');
      return;
    }

    this.isUpdating = true;
    const connection = this.createConnection();

    try {
      console.log('🔄 开始更新汇率数据...');
      console.log(`📊 支持 ${this.supportedCurrencies.length} 种货币到 ${this.baseCurrency} 的汇率`);

      // 开始事务
      await this.safeRun(connection, "BEGIN TRANSACTION");

      let successCount = 0;
      let errorCount = 0;

      // 为每种货币获取到CNY的汇率
      for (const fromCurrency of this.supportedCurrencies) {
        try {
          // 跳过相同的货币对
          if (fromCurrency === this.baseCurrency) {
            continue;
          }

          const rate = await this.API_FetchExRate(fromCurrency, this.baseCurrency);

          // 插入或更新汇率
          await this.safeRun(connection, `
            INSERT OR REPLACE INTO tblExchangeRateTTM (fromCurrency, toCurrency, rate, lastUpdated)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
          `, [fromCurrency, this.baseCurrency, rate]);

          successCount++;
          console.log(`✅ ${fromCurrency} -> ${this.baseCurrency}: ${rate.toFixed(4)}`);

        } catch (error) {
          errorCount++;
          console.error(`❌ 更新 ${fromCurrency} -> ${this.baseCurrency} 汇率失败:`, error.message);
        }

        // 添加小延迟，避免API限制
        await new Promise(resolve => setTimeout(resolve, 500));
      }

      // 添加CNY到CNY的汇率（总是1.0）
      try {
        await this.safeRun(connection, `
          INSERT OR REPLACE INTO tblExchangeRateTTM (fromCurrency, toCurrency, rate, lastUpdated)
          VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        `, [this.baseCurrency, this.baseCurrency, 1.0]);
        successCount++;
        console.log(`✅ ${this.baseCurrency} -> ${this.baseCurrency}: 1.0000`);
      } catch (error) {
        errorCount++;
        console.error(`❌ 更新 ${this.baseCurrency} -> ${this.baseCurrency} 汇率失败:`, error.message);
      }

      // 提交事务
      await this.safeRun(connection, "COMMIT");

      console.log(`✅ 汇率更新完成: ${successCount} 成功, ${errorCount} 失败`);

      // 更新统计信息
      await this.updateExchangeRateStats(connection);

      return {
        total: this.supportedCurrencies.length + 1, // +1 for CNY to CNY
        success: successCount,
        error: errorCount
      };

    } catch (error) {
      // 回滚事务
      try {
        await this.safeRun(connection, "ROLLBACK");
      } catch (rollbackError) {
        console.warn('回滚事务时出错:', rollbackError.message);
      }

      console.error('❌ 汇率更新失败:', error.message);
      throw error;
    } finally {
      this.isUpdating = false;
      this.closeConnection(connection);
    }
  }

  /**
   * 更新汇率统计信息
   */
  async updateExchangeRateStats(connection) {
    try {
      const stats = await this.safeQuery(connection, `
        SELECT 
          COUNT(*) as totalRates,
          MIN(lastUpdated) as oldestUpdate,
          MAX(lastUpdated) as newestUpdate,
          COUNT(CASE WHEN lastUpdated >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR THEN 1 END) as updatedLastHour
        FROM tblExchangeRateTTM
      `);

      console.log('\n📈 汇率数据统计:');
      console.log(`   总汇率数量: ${stats[0]?.totalRates || 0}`);
      console.log(`   最近1小时更新: ${stats[0]?.updatedLastHour || 0}`);
      console.log(`   最早更新时间: ${stats[0]?.oldestUpdate || 'N/A'}`);
      console.log(`   最新更新时间: ${stats[0]?.newestUpdate || 'N/A'}`);

    } catch (error) {
      console.warn('⚠️ 更新汇率统计失败:', error.message);
      await this.updateExchangeRateStatsFallback(connection);
    }
  }

  /**
   * 备用统计方法
   */
  async updateExchangeRateStatsFallback(connection) {
    try {
      const stats = await this.safeQuery(connection, `
        SELECT 
          COUNT(*) as totalRates,
          MIN(lastUpdated) as oldestUpdate,
          MAX(lastUpdated) as newestUpdate
        FROM tblExchangeRateTTM
      `);

      console.log('\n📈 汇率数据统计(基础版):');
      console.log(`   总汇率数量: ${stats[0]?.totalRates || 0}`);
      console.log(`   最早更新时间: ${stats[0]?.oldestUpdate || 'N/A'}`);
      console.log(`   最新更新时间: ${stats[0]?.newestUpdate || 'N/A'}`);

    } catch (error) {
      console.warn('⚠️ 备用统计方法也失败:', error.message);
    }
  }

  /**
   * 获取特定货币对的汇率
   */
  async getExchangeRate(from, to) {
    const connection = this.createConnection();

    try {
      const result = await this.safeQuery(connection,
        "SELECT rate, lastUpdated FROM tblExchangeRateTTM WHERE fromCurrency = ? AND toCurrency = ?",
        [from, to]
      );

      return result[0] || null;

    } catch (error) {
      console.error(`❌ 获取 ${from}->${to} 汇率失败:`, error.message);
      return null;
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 获取所有汇率列表
   */
  async getAllExchangeRates() {
    const connection = this.createConnection();

    try {
      const result = await this.safeQuery(connection,
        "SELECT fromCurrency, toCurrency, rate, lastUpdated FROM tblExchangeRateTTM ORDER BY fromCurrency, toCurrency"
      );

      return result;

    } catch (error) {
      console.error('❌ 获取所有汇率失败:', error.message);
      return [];
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 启动定时任务
   */
  startScheduledTask(cronExpression = '0 0 */6 * * *') { // 默认每6小时执行一次
    console.log(`⏰ 启动定时汇率更新任务，计划: ${cronExpression}`);

    nodeCron.schedule(cronExpression, async () => {
      console.log('\n🔄 定时执行汇率更新...');
      try {
        await this.updateExchangeRate();
        console.log('✅ 定时汇率更新完成');
      } catch (error) {
        console.error('❌ 定时汇率更新失败:', error.message);
      }
    });

    console.log('✅ 定时汇率更新任务已启动');
  }

  /**
   * 立即执行一次汇率更新
   */
  async executeImmediately() {
    try {
      await this.updateExchangeRate();
    } catch (error) {
      console.error('❌ 立即执行汇率更新失败:', error.message);
      throw error;
    }
  }

  /**
   * 添加新的支持货币
   */
  addSupportedCurrency(currency) {
    if (!this.supportedCurrencies.includes(currency)) {
      this.supportedCurrencies.push(currency);
      console.log(`✅ 添加支持货币: ${currency}`);
    }
  }

  /**
   * 设置基准货币
   */
  setBaseCurrency(currency) {
    this.baseCurrency = currency;
    console.log(`✅ 设置基准货币为: ${currency}`);
  }

  /**
   * 优雅关闭
   */
  shutdown() {
    console.log('🛑 停止汇率更新服务...');
    console.log('✅ 汇率更新服务已停止');
  }
}

/**
 * 主函数
 */
async function main() {
  console.log('🚀 启动汇率更新服务...');

  const exchangeRateService = new ExchangeRateUpdateService();

  // 注册关闭信号
  process.on('SIGINT', () => {
    exchangeRateService.shutdown();
    process.exit(0);
  });

  process.on('SIGTERM', () => {
    exchangeRateService.shutdown();
    process.exit(0);
  });

  try {
    // 启动定时任务（默认每6小时执行一次）
    const cronExpression = process.env.EXCHANGE_RATE_CRON || '0 0 */6 * * *';
    exchangeRateService.startScheduledTask(cronExpression);

    // 如果指定了立即执行参数
    if (process.argv.includes('--immediate')) {
      console.log('⚡ 立即执行汇率更新...');
      await exchangeRateService.executeImmediately();
      console.log('✅ 立即执行完成，退出进程');
      process.exit(0); // 立即执行完成后退出
    }

    // 如果指定了查询特定汇率
    const fromIndex = process.argv.indexOf('--from');
    const toIndex = process.argv.indexOf('--to');
    if (fromIndex !== -1 && toIndex !== -1 && process.argv[fromIndex + 1] && process.argv[toIndex + 1]) {
      const from = process.argv[fromIndex + 1];
      const to = process.argv[toIndex + 1];
      console.log(`🔍 查询汇率 ${from} -> ${to}`);
      const rate = await exchangeRateService.getExchangeRate(from, to);
      if (rate) {
        console.log(`💱 ${from} -> ${to}: ${rate.rate} (更新于: ${rate.lastUpdated})`);
      } else {
        console.log(`❌ 未找到 ${from} -> ${to} 的汇率数据`);
      }
      process.exit(0);
    }

    // 如果指定了显示所有汇率
    if (process.argv.includes('--list')) {
      console.log('🔍 显示所有汇率:');
      const allRates = await exchangeRateService.getAllExchangeRates();
      allRates.forEach(rate => {
        console.log(`   ${rate.fromCurrency} -> ${rate.toCurrency}: ${rate.rate} (${rate.lastUpdated})`);
      });
      process.exit(0);
    }

    // 如果指定了添加新货币
    const addIndex = process.argv.indexOf('--add-currency');
    if (addIndex !== -1 && process.argv[addIndex + 1]) {
      const currency = process.argv[addIndex + 1];
      exchangeRateService.addSupportedCurrency(currency);
      console.log(`✅ 已添加货币 ${currency}，下次更新时将包含该货币`);
      process.exit(0);
    }

    console.log('✅ 汇率更新服务运行中...');
    console.log('💡 使用 Ctrl+C 停止服务');

    // 保持进程运行
    setInterval(() => {
      // 心跳检测，保持进程活跃
    }, 60000);

  } catch (error) {
    console.error('❌ 汇率更新服务启动失败:', error.message);
    exchangeRateService.shutdown();
    process.exit(1);
  }
}

// 运行主函数
if (require.main === module) {
  main().catch(console.error);
}

module.exports = ExchangeRateUpdateService;
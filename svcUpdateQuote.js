// svcUpdateQuote.js (修复版本)
const duckdb = require('duckdb');
const nodeCron = require('node-cron');
const APIModuleYahoo = require("./API_YFinance") ;

const duckDbFilePath = './portfolioData.duckdb';

class QuoteUpdateService {
  constructor() {
    this.dbInstance = new duckdb.Database(duckDbFilePath);
    this.isUpdating = false;
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
   * 模拟报价API调用函数
   * 在实际使用中，这里应该替换为真实的API调用
   */
  async _API_FetchQuote(ticker) {
    // 模拟API调用延迟

    if(ticker=="US_TBill")return 1 ;
    if(ticker == "515080.SS" || ticker == "515180.SS") return 1 ;
    if(ticker =="BF B")ticker = "BF-B" ;
    if(ticker =="BRK B")ticker = "BRK-B" ;

    let basePrice = await APIModuleYahoo.API_FetchQuote(ticker);
    if (ticker.endsWith('.L')) {
      basePrice = basePrice/100; // LSE股价调整为英镑
    }
    /*
    if (ticker.endsWith('.HK')) {
      basePrice = 10 + Math.random() * 90; // 港股价格范围
    } else if (ticker.endsWith('.SS') || ticker.endsWith('.SZ')) {
      basePrice = 5 + Math.random() * 95; // A股价格范围
    }else if (ticker.endsWith('.L')) {
      basePrice = 5 + Math.random() * 95; // A股价格范围
    } else {
      basePrice = 20 + Math.random() * 180; // 美股等其他市场
    }
    
    // 添加小幅随机波动 (±5%)
    const fluctuation = (Math.random() - 0.5) * 0.1;
    const price = basePrice * (1 + fluctuation);
    
    console.log(`📡 获取 ${ticker} 报价: ${price.toFixed(2)}`);
    */
    
    //return parseFloat(price.toFixed(4));
    console.log(`${ticker}===>${basePrice}`) ;
    return basePrice ;
  }

  /**
   * 获取所有需要更新报价的ticker列表
   */
  async getAllTickers() {
    const connection = this.createConnection();
    
    try {
      const result = await this.safeQuery(connection, `
        SELECT DISTINCT ticker 
        FROM tblAccountHoldings 
        WHERE ticker NOT LIKE 'CASH_%' 
        AND ticker NOT LIKE 'US_TBill'
        ORDER BY ticker
      `);
      
      return result.map(row => row.ticker);
      
    } catch (error) {
      console.error('❌ 获取ticker列表失败:', error.message);
      return [];
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 批量更新报价数据
   */
  async updateQuotes() {
    if (this.isUpdating) {
      console.log('⚠️ 报价更新正在进行中，跳过本次执行');
      return;
    }

    this.isUpdating = true;
    const connection = this.createConnection();
    
    try {
      console.log('🔄 开始更新报价数据...');
      
      // 获取所有需要更新的ticker
      const tickers = await this.getAllTickers();
      console.log(`📊 找到 ${tickers.length} 个需要更新报价的标的`);
      
      if (tickers.length === 0) {
        console.log('ℹ️ 没有找到需要更新报价的标的');
        return;
      }

      // 开始事务
      await this.safeRun(connection, "BEGIN TRANSACTION");

      let successCount = 0;
      let errorCount = 0;
      const batchSize = 5; // 控制并发数量，避免API限制
      
      // 分批处理，避免过多并发请求
      for (let i = 0; i < tickers.length; i += batchSize) {
        const batch = tickers.slice(i, i + batchSize);
        console.log(`📦 处理批次 ${Math.floor(i/batchSize) + 1}/${Math.ceil(tickers.length/batchSize)}: ${batch.join(', ')}`);
        
        // 并行获取报价
        const batchPromises = batch.map(async (ticker) => {
          try {
            //const price = await this._API_FetchQuote(ticker);
            
            // 获取货币信息（从持仓表中获取）
            const currencyResult = await this.safeQuery(connection, 
              "SELECT currency FROM tblAccountHoldings WHERE ticker = ? LIMIT 1", 
              [ticker]
            );
            
            const currency = currencyResult[0]?.currency || 'USD';

            let price = 0 ;
            if(currency == 'GBP'){
              let tickerLSE = `${ticker}.L` ;
              if(ticker=='INPPl'){tickerLSE = 'INPP.L' ;}
              price = await this._API_FetchQuote(tickerLSE);
            }else if(currency == 'CAD'){
              let tickerCA =`${ticker}.TO` ;
              if(ticker =='ENB.PR.B'){
                price=18.01 ;
              }else if(ticker =='FTS.PR.G'){
                price = 22.31 ;
              }else{
                price = await this._API_FetchQuote(tickerCA);
              }
            }else{
              price = await this._API_FetchQuote(ticker);
            }
            
            
            // 插入或更新报价
            await this.safeRun(connection, `
              INSERT OR REPLACE INTO tblQuotationTTM (ticker, price, currency, lastUpdated)
              VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            `, [ticker, price, currency]);
            
            successCount++;
            return { ticker, success: true, price };
            
          } catch (error) {
            errorCount++;
            console.error(`❌ 更新 ${ticker} 报价失败:`, error.message);
            return { ticker, success: false, error: error.message };
          }
        });

        // 等待当前批次完成
        const batchResults = await Promise.all(batchPromises);
        
        // 显示批次结果
        const batchSuccess = batchResults.filter(r => r.success).length;
        const batchError = batchResults.filter(r => !r.success).length;
        console.log(`   ✅ 成功: ${batchSuccess}, ❌ 失败: ${batchError}`);
        
        // 批次间延迟，避免API限制
        if (i + batchSize < tickers.length) {
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
      }

      // 提交事务
      await this.safeRun(connection, "COMMIT");

      console.log(`✅ 报价更新完成: ${successCount} 成功, ${errorCount} 失败`);
      
      // 更新统计信息
      await this.updateQuoteStats(connection);

      return {
        total: tickers.length,
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
      
      console.error('❌ 报价更新失败:', error.message);
      throw error;
    } finally {
      this.isUpdating = false;
      this.closeConnection(connection);
    }
  }

  /**
   * 更新报价统计信息 - 使用DuckDB兼容的时间函数
   */
  async updateQuoteStats(connection) {
    try {
      // 使用DuckDB兼容的时间函数
      const stats = await this.safeQuery(connection, `
        SELECT 
          COUNT(*) as totalQuotes,
          MIN(lastUpdated) as oldestUpdate,
          MAX(lastUpdated) as newestUpdate,
          COUNT(CASE WHEN lastUpdated >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR THEN 1 END) as updatedLastHour
        FROM tblQuotationTTM
      `);

      console.log('\n📈 报价数据统计:');
      console.log(`   总报价数量: ${stats[0]?.totalQuotes || 0}`);
      console.log(`   最近1小时更新: ${stats[0]?.updatedLastHour || 0}`);
      console.log(`   最早更新时间: ${stats[0]?.oldestUpdate || 'N/A'}`);
      console.log(`   最新更新时间: ${stats[0]?.newestUpdate || 'N/A'}`);

    } catch (error) {
      console.warn('⚠️ 更新报价统计失败:', error.message);
      // 尝试使用备用统计方法
      await this.updateQuoteStatsFallback(connection);
    }
  }

  /**
   * 备用统计方法 - 不使用时间函数
   */
  async updateQuoteStatsFallback(connection) {
    try {
      const stats = await this.safeQuery(connection, `
        SELECT 
          COUNT(*) as totalQuotes,
          MIN(lastUpdated) as oldestUpdate,
          MAX(lastUpdated) as newestUpdate
        FROM tblQuotationTTM
      `);

      console.log('\n📈 报价数据统计(基础版):');
      console.log(`   总报价数量: ${stats[0]?.totalQuotes || 0}`);
      console.log(`   最早更新时间: ${stats[0]?.oldestUpdate || 'N/A'}`);
      console.log(`   最新更新时间: ${stats[0]?.newestUpdate || 'N/A'}`);

    } catch (error) {
      console.warn('⚠️ 备用统计方法也失败:', error.message);
    }
  }

  /**
   * 获取单个ticker的报价（工具函数）
   */
  async getQuote(ticker) {
    const connection = this.createConnection();
    
    try {
      const result = await this.safeQuery(connection, 
        "SELECT price, currency, lastUpdated FROM tblQuotationTTM WHERE ticker = ?", 
        [ticker]
      );
      
      return result[0] || null;
      
    } catch (error) {
      console.error(`❌ 获取 ${ticker} 报价失败:`, error.message);
      return null;
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 获取最近更新的报价列表
   */
  async getRecentQuotes(limit = 10) {
    const connection = this.createConnection();
    
    try {
      const result = await this.safeQuery(connection, 
        "SELECT ticker, price, currency, lastUpdated FROM tblQuotationTTM ORDER BY lastUpdated DESC LIMIT ?", 
        [limit]
      );
      
      return result;
      
    } catch (error) {
      console.error('❌ 获取最近报价失败:', error.message);
      return [];
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 启动定时任务
   */
  startScheduledTask(cronExpression = '0 */5 * * * *') { // 默认每5分钟执行一次
    console.log(`⏰ 启动定时报价更新任务，计划: ${cronExpression}`);
    
    nodeCron.schedule(cronExpression, async () => {
      console.log('\n🔄 定时执行报价更新...');
      try {
        await this.updateQuotes();
        console.log('✅ 定时报价更新完成');
      } catch (error) {
        console.error('❌ 定时报价更新失败:', error.message);
      }
    });
    
    console.log('✅ 定时报价更新任务已启动');
  }

  /**
   * 立即执行一次报价更新
   */
  async executeImmediately() {
    try {
      await this.updateQuotes();
    } catch (error) {
      console.error('❌ 立即执行报价更新失败:', error.message);
      throw error;
    }
  }

  /**
   * 优雅关闭
   */
  shutdown() {
    console.log('🛑 停止报价更新服务...');
    // 这里可以添加清理逻辑
    console.log('✅ 报价更新服务已停止');
  }
}

/**
 * 主函数
 */
async function main() {
  console.log('🚀 启动报价更新服务...');
  
  const quoteService = new QuoteUpdateService();
  
  // 注册关闭信号
  process.on('SIGINT', () => {
    quoteService.shutdown();
    process.exit(0);
  });
  
  process.on('SIGTERM', () => {
    quoteService.shutdown();
    process.exit(0);
  });

  try {
    // 如果指定了立即执行参数
    if (process.argv.includes('--immediate')) {
      console.log('⚡ 立即执行报价更新...');
      await quoteService.executeImmediately();
      console.log('✅ 立即执行完成，退出进程');
      process.exit(0); // 立即执行完成后退出
    }
    
    // 如果指定了单个ticker查询
    const tickerIndex = process.argv.indexOf('--ticker');
    if (tickerIndex !== -1 && process.argv[tickerIndex + 1]) {
      const ticker = process.argv[tickerIndex + 1];
      console.log(`🔍 查询单个ticker: ${ticker}`);
      const quote = await quoteService.getQuote(ticker);
      if (quote) {
        console.log(`💰 ${ticker}: ${quote.price} ${quote.currency} (更新于: ${quote.lastUpdated})`);
      } else {
        console.log(`❌ 未找到 ${ticker} 的报价数据`);
      }
      process.exit(0);
    }
    
    // 如果指定了显示最近报价
    if (process.argv.includes('--recent')) {
      const limit = process.argv[process.argv.indexOf('--recent') + 1] || 10;
      console.log(`🔍 显示最近 ${limit} 个更新的报价:`);
      const recentQuotes = await quoteService.getRecentQuotes(parseInt(limit));
      recentQuotes.forEach(quote => {
        console.log(`   ${quote.ticker}: ${quote.price} ${quote.currency} (${quote.lastUpdated})`);
      });
      process.exit(0);
    }
    
    // 如果没有特殊参数，启动定时任务（默认每5分钟执行一次）
    const cronExpression = process.env.QUOTE_UPDATE_CRON || '0 */5 * * * *';
    quoteService.startScheduledTask(cronExpression);
    
    console.log('✅ 报价更新服务运行中...');
    console.log('💡 使用 Ctrl+C 停止服务');
    
    // 保持进程运行
    setInterval(() => {
      // 心跳检测，保持进程活跃
    }, 60000);
    
  } catch (error) {
    console.error('❌ 报价更新服务启动失败:', error.message);
    quoteService.shutdown();
    process.exit(1);
  }
}

// 运行主函数
if (require.main === module) {
  main().catch(console.error);
}

module.exports = QuoteUpdateService;
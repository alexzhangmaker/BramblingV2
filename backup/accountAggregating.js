// accountAggregating.js
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

class AccountAggregating {
  constructor() {
    this.checkEnvironment();
    
    this.supabase = createClient(
      process.env.SUPABASE_URL,
      process.env.SUPABASE_ANON_KEY,
      {
        auth: {
          persistSession: false,
          autoRefreshToken: false
        }
      }
    );
    
    this.authenticated = false;
    
    // 支持的账户表列表
    this.accountTables = [
        'account_IB7075',
        'account_IB1279',
        'account_IB3979',
        'account_IB6325',
        'account_HTZQ',
        'account_GJZQ',
        'account_PAZQ',
        'account_ZSZQ',
        'account_ZSXG',
        'account_FTZQ',
        'account_LHZQ'
    ];
  }

  // 检查环境变量
  checkEnvironment() {
    console.log('🔍 检查环境变量...');
    console.log('SUPABASE_URL:', process.env.SUPABASE_URL ? '已设置' : '❌ 未设置');
    console.log('SUPABASE_ANON_KEY:', process.env.SUPABASE_ANON_KEY ? '已设置' : '❌ 未设置');
    
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_ANON_KEY) {
      throw new Error('请检查 .env 文件中的 SUPABASE_URL 和 SUPABASE_ANON_KEY 配置');
    }
  }

  async initialize() {
    console.log('🔐 初始化账户汇总处理器...');
    
    try {
      // 测试连接
      const { error: testError } = await this.supabase
        .from('account_Holdings')
        .select('count')
        .limit(1);

      if (testError && testError.code !== 'PGRST116') {
        console.log('连接测试结果:', testError.message);
      } else {
        console.log('✅ Supabase 连接正常');
      }

      // 尝试登录（可选）
      if (process.env.SERVICE_ACCOUNT_EMAIL && process.env.SERVICE_ACCOUNT_PASSWORD) {
        const { data, error } = await this.supabase.auth.signInWithPassword({
          email: process.env.SERVICE_ACCOUNT_EMAIL,
          password: process.env.SERVICE_ACCOUNT_PASSWORD
        });

        if (error) {
          console.log('⚠️ 登录失败，继续以匿名模式运行:', error.message);
        } else {
          this.authenticated = true;
          console.log('✅ 登录成功:', data.user.email);
        }
      }
      
      return true;
      
    } catch (error) {
      console.error('❌ 初始化异常:', error.message);
      return false;
    }
  }

  // 1. 持仓汇总处理流程
  async aggregateHoldings() {
    console.log('\n🔄 开始持仓汇总处理...');
    const startTime = Date.now();

    try {
      // 步骤1: 从所有账户表获取持仓数据
      console.log('📊 从各账户表收集持仓数据...');
      const allHoldings = await this.collectAllHoldings();
      
      if (!allHoldings || allHoldings.length === 0) {
        console.log('✅ 没有找到持仓数据');
        return { success: true, message: '没有持仓数据需要汇总' };
      }

      // 步骤2: 按ticker汇总计算
      console.log('🧮 按ticker进行汇总计算...');
      const aggregatedData = this.calculateAggregations(allHoldings);
      
      // 步骤3: 写入account_Holdings表
      console.log('💾 写入汇总数据到account_Holdings表...');
      const result = await this.updateHoldingsTable(aggregatedData);
      
      const duration = Date.now() - startTime;
      console.log(`✅ 持仓汇总完成! 处理了 ${Object.keys(aggregatedData).length} 个ticker, 耗时 ${duration}ms`);
      
      return {
        success: true,
        tickersProcessed: Object.keys(aggregatedData).length,
        duration: duration,
        details: result
      };
      
    } catch (error) {
      const duration = Date.now() - startTime;
      console.error(`❌ 持仓汇总失败:`, error.message);
      
      return {
        success: false,
        error: error.message,
        duration: duration
      };
    }
  }

  // 从所有账户表收集持仓数据
  async collectAllHoldings() {
    const allHoldings = [];
    
    for (const tableName of this.accountTables) {
      try {
        console.log(`   正在查询表: ${tableName}...`);
        const { data, error } = await this.supabase
          .from(tableName)
          .select('ticker, company, holding, costPerShare, quoteType')
          .gt('holding', 0); // 只获取有持仓的记录

        if (error) {
          console.log(`   ⚠️ 查询表 ${tableName} 失败:`, error.message);
          continue;
        }

        if (data && data.length > 0) {
          // 为每条记录添加来源表信息
          const holdingsWithSource = data.map(record => ({
            ...record,
            sourceTable: tableName
          }));
          allHoldings.push(...holdingsWithSource);
          console.log(`   ✅ 从 ${tableName} 获取到 ${data.length} 条持仓记录`);
        } else {
          console.log(`   ℹ️ 表 ${tableName} 没有持仓记录`);
        }
        
      } catch (error) {
        console.log(`   ❌ 处理表 ${tableName} 时出错:`, error.message);
      }
    }
    
    console.log(`   总计收集到 ${allHoldings.length} 条持仓记录`);
    return allHoldings;
  }

  // 按ticker进行汇总计算
  calculateAggregations(holdings) {
    const tickerMap = {};
    
    holdings.forEach(holding => {
      const { ticker, company, holding: quantity, costPerShare, quoteType } = holding;
      
      if (!tickerMap[ticker]) {
        // 初始化ticker记录
        tickerMap[ticker] = {
          ticker,
          company: company || `${ticker} Company`,
          totalHolding: 0,
          totalCost: 0,
          quoteTypes: new Set(),
          sources: new Set()
        };
      }
      
      const record = tickerMap[ticker];
      const positionCost = quantity * costPerShare;
      
      // 累加持仓和成本
      record.totalHolding += quantity;
      record.totalCost += positionCost;
      record.quoteTypes.add(quoteType || 'equity');
      record.sources.add(holding.sourceTable);
    });
    
    // 计算加权平均成本和其他字段
    const aggregatedData = {};
    Object.values(tickerMap).forEach(record => {
      const costPerShare = record.totalHolding > 0 ? record.totalCost / record.totalHolding : 0;
      const quoteType = Array.from(record.quoteTypes).join(',');
      
      aggregatedData[record.ticker] = {
        ticker: record.ticker,
        company: record.company,
        holding: record.totalHolding,
        costPerShare: parseFloat(costPerShare.toFixed(4)),
        total_cost: parseFloat(record.totalCost.toFixed(2)),
        quote: 0, // 初始报价为0，需要后续更新
        current_value: 0, // 初始市值为0
        pct_gain_loss: 0, // 初始损益为0
        quoteType: quoteType
      };
    });
    
    console.log(`   汇总计算完成: ${Object.keys(aggregatedData).length} 个唯一ticker`);
    return aggregatedData;
  }

  // 更新account_Holdings表
  async updateHoldingsTable(aggregatedData) {
    const tickers = Object.keys(aggregatedData);
    const results = {
      inserted: 0,
      updated: 0,
      errors: 0,
      details: []
    };

    for (const ticker of tickers) {
      try {
        const holdingData = aggregatedData[ticker];
        
        // 使用upsert操作（存在则更新，不存在则插入）
        const { data, error } = await this.supabase
          .from('account_Holdings')
          .upsert(holdingData, { 
            onConflict: 'ticker',
            ignoreDuplicates: false
          })
          .select();

        if (error) {
          console.log(`   ❌ 更新 ${ticker} 失败:`, error.message);
          results.errors++;
          results.details.push({ ticker, status: 'error', error: error.message });
        } else {
          if (data && data.length > 0) {
            const operation = data[0].created_at === data[0].updated_at ? 'inserted' : 'updated';
            results[operation]++;
            results.details.push({ ticker, status: operation });
          }
        }
        
      } catch (error) {
        console.log(`   ❌ 处理 ${ticker} 时异常:`, error.message);
        results.errors++;
        results.details.push({ ticker, status: 'error', error: error.message });
      }
    }
    
    console.log(`   表更新完成: 插入 ${results.inserted}, 更新 ${results.updated}, 错误 ${results.errors}`);
    return results;
  }

  // 2. 实时更新报价和市值
  async updateQuote(ticker, quote) {
    console.log(`\n📈 更新报价: ${ticker} -> $${quote}`);
    const startTime = Date.now();

    try {
      // 步骤1: 获取当前持仓信息
      const { data: holding, error: fetchError } = await this.supabase
        .from('account_Holdings')
        .select('holding, costPerShare, total_cost')
        .eq('ticker', ticker)
        .single();

      if (fetchError) {
        if (fetchError.code === 'PGRST116') {
          throw new Error(`ticker ${ticker} 在 account_Holdings 表中不存在`);
        }
        throw fetchError;
      }

      if (!holding) {
        throw new Error(`未找到 ticker ${ticker} 的持仓信息`);
      }

      // 步骤2: 计算市值和损益
      const calculations = this.calculateValueAndGainLoss(holding, quote);
      
      // 步骤3: 更新记录
      const updateData = {
        quote: parseFloat(quote.toFixed(4)),
        current_value: calculations.currentValue,
        pct_gain_loss: calculations.gainLossPercentage,
        updated_at: new Date().toISOString()
      };

      const { data, error: updateError } = await this.supabase
        .from('account_Holdings')
        .update(updateData)
        .eq('ticker', ticker)
        .select();

      if (updateError) {
        throw updateError;
      }

      const duration = Date.now() - startTime;
      console.log(`✅ 报价更新成功: ${ticker}`);
      console.log(`   持仓: ${holding.holding}股, 成本: $${holding.costPerShare.toFixed(2)}`);
      console.log(`   报价: $${quote.toFixed(2)}, 市值: $${calculations.currentValue.toFixed(2)}`);
      console.log(`   损益: ${calculations.gainLossPercentage.toFixed(2)}%`);
      
      return {
        success: true,
        ticker: ticker,
        quote: quote,
        current_value: calculations.currentValue,
        pct_gain_loss: calculations.gainLossPercentage,
        duration: duration
      };
      
    } catch (error) {
      const duration = Date.now() - startTime;
      console.error(`❌ 更新报价失败:`, error.message);
      
      return {
        success: false,
        ticker: ticker,
        error: error.message,
        duration: duration
      };
    }
  }

  // 批量更新多个ticker的报价
  async updateQuotesBatch(quoteUpdates) {
    console.log('\n📊 批量更新报价...');
    const startTime = Date.now();
    const results = {
      success: 0,
      failed: 0,
      details: []
    };

    for (const update of quoteUpdates) {
      const { ticker, quote } = update;
      const result = await this.updateQuote(ticker, quote);
      
      if (result.success) {
        results.success++;
      } else {
        results.failed++;
      }
      
      results.details.push(result);
      
      // 添加小延迟避免过快请求
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    const duration = Date.now() - startTime;
    console.log(`✅ 批量更新完成: 成功 ${results.success}, 失败 ${results.failed}, 耗时 ${duration}ms`);
    
    return results;
  }

  // 计算市值和损益
  calculateValueAndGainLoss(holding, quote) {
    const { holding: quantity, costPerShare, total_cost } = holding;
    
    const currentValue = quantity * quote;
    const totalCost = total_cost || (quantity * costPerShare);
    
    let gainLossPercentage = 0;
    if (totalCost > 0) {
      gainLossPercentage = ((currentValue - totalCost) / totalCost) * 100;
    }
    
    return {
      currentValue: parseFloat(currentValue.toFixed(2)),
      gainLossPercentage: parseFloat(gainLossPercentage.toFixed(2))
    };
  }

  // 获取汇总统计信息
  async getAggregationStats() {
    try {
      // 获取account_Holdings表的统计
      const { data: holdings, error: holdingsError } = await this.supabase
        .from('account_Holdings')
        .select('*');

      if (holdingsError) throw holdingsError;

      const stats = {
        totalTickers: holdings.length,
        totalHoldingValue: 0,
        totalCurrentValue: 0,
        totalGainLoss: 0,
        byQuoteType: {}
      };

      holdings.forEach(holding => {
        stats.totalHoldingValue += holding.total_cost || 0;
        stats.totalCurrentValue += holding.current_value || 0;
        stats.totalGainLoss += (holding.current_value - holding.total_cost) || 0;
        
        const quoteType = holding.quoteType || 'unknown';
        stats.byQuoteType[quoteType] = (stats.byQuoteType[quoteType] || 0) + 1;
      });

      stats.totalGainLossPercentage = stats.totalHoldingValue > 0 ? 
        (stats.totalGainLoss / stats.totalHoldingValue) * 100 : 0;

      return {
        success: true,
        stats: stats
      };
      
    } catch (error) {
      console.error('❌ 获取统计信息失败:', error.message);
      return {
        success: false,
        error: error.message
      };
    }
  }

  // 添加新的账户表
  addAccountTable(tableName) {
    if (!this.accountTables.includes(tableName)) {
      this.accountTables.push(tableName);
      console.log(`✅ 添加账户表: ${tableName}`);
    }
    return this.accountTables;
  }

  // 显示支持的账户表
  showAccountTables() {
    console.log('\n📋 支持的账户表:');
    this.accountTables.forEach(table => {
      console.log(`  - ${table}`);
    });
  }
}

// 使用示例和测试函数
async function runAggregationDemo() {
  console.log('🧪 账户汇总演示\n');
  
  const aggregator = new AccountAggregating();
  const initialized = await aggregator.initialize();
  
  if (!initialized) {
    console.log('❌ 初始化失败');
    return;
  }

  // 显示支持的账户表
  aggregator.showAccountTables();

  // 1. 执行持仓汇总
  console.log('\n1. 执行持仓汇总...');
  const aggregationResult = await aggregator.aggregateHoldings();
  console.log('汇总结果:', aggregationResult);

  // 2. 模拟更新报价（演示用）
  console.log('\n2. 模拟更新报价...');
  const testQuotes = [
    { ticker: 'AAPL', quote: 185.50 },
    { ticker: 'GOOGL', quote: 2850.75 },
    { ticker: 'TSLA', quote: 245.30 }
  ];
  
  const quoteResults = await aggregator.updateQuotesBatch(testQuotes);
  console.log('报价更新结果:', quoteResults);

  // 3. 获取统计信息
  console.log('\n3. 获取汇总统计...');
  const stats = await aggregator.getAggregationStats();
  if (stats.success) {
    console.log('汇总统计:', stats.stats);
  }

  console.log('\n✅ 演示完成!');
}

// 导出模块
module.exports = {
  AccountAggregating,
  runAggregationDemo
};

// 如果直接运行此文件，执行演示
if (require.main === module) {
  runAggregationDemo().catch(console.error);
}
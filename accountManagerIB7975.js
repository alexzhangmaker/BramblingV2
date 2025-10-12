// account-ib7075-manager.js
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

class AccountIB7075Manager {
  constructor() {
    this.supabase = createClient(
      process.env.SUPABASE_URL,
      process.env.SUPABASE_ANON_KEY
    );
    this.authenticated = false;
    this.tableName = 'account_IB7075';
  }

  async initialize() {
    console.log('🔐 初始化 IB7075 账户管理器...');
    
    try {
      // 使用 Service Account 登录
      const { data, error } = await this.supabase.auth.signInWithPassword({
        email: process.env.SERVICE_ACCOUNT_EMAIL,
        password: process.env.SERVICE_ACCOUNT_PASSWORD
      });

      if (error) throw error;
      
      this.authenticated = true;
      console.log('✅ 登录成功:', data.user.email);
      return true;
      
    } catch (error) {
      console.error('❌ 初始化失败:', error.message);
      return false;
    }
  }

  // 创建或更新持仓记录
  async upsertHolding(holdingData) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from(this.tableName)
        .upsert(holdingData, { onConflict: 'ticker' })
        .select();

      if (error) throw error;
      
      console.log('✅ 持仓记录保存成功, 代码:', holdingData.ticker);
      return data[0];
      
    } catch (error) {
      console.error('❌ 保存持仓记录失败:', error.message);
      return null;
    }
  }

  // 批量创建或更新持仓记录
  async upsertMultipleHoldings(holdingsArray) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from(this.tableName)
        .upsert(holdingsArray, { onConflict: 'ticker' })
        .select();

      if (error) throw error;
      
      console.log(`✅ 批量保存成功, 共处理 ${data.length} 条记录`);
      return data;
      
    } catch (error) {
      console.error('❌ 批量保存失败:', error.message);
      return null;
    }
  }

  // 获取所有持仓记录
  async getAllHoldings(sortBy = 'ticker') {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from(this.tableName)
        .select('*')
        .order(sortBy, { ascending: true });

      if (error) throw error;
      
      console.log(`✅ 获取到 ${data.length} 条持仓记录`);
      return data;
      
    } catch (error) {
      console.error('❌ 读取持仓记录失败:', error.message);
      return null;
    }
  }

  // 根据代码查询特定持仓
  async getHoldingByTicker(ticker) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from(this.tableName)
        .select('*')
        .eq('ticker', ticker)
        .single();

      if (error) throw error;
      
      console.log('✅ 查询成功:', ticker);
      return data;
      
    } catch (error) {
      console.error('❌ 查询持仓失败:', error.message);
      return null;
    }
  }

  // 根据条件查询持仓
  async getHoldingsByCondition(conditions = {}) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      let query = this.supabase
        .from(this.tableName)
        .select('*');

      // 动态添加查询条件
      if (conditions.currency) {
        query = query.eq('currency', conditions.currency);
      }
      if (conditions.quoteType) {
        query = query.eq('quoteType', conditions.quoteType);
      }
      if (conditions.exchange) {
        query = query.eq('exchange', conditions.exchange);
      }
      if (conditions.minHolding) {
        query = query.gte('holding', conditions.minHolding);
      }
      if (conditions.maxHolding) {
        query = query.lte('holding', conditions.maxHolding);
      }

      query = query.order('ticker', { ascending: true });

      const { data, error } = await query;

      if (error) throw error;
      
      console.log(`✅ 条件查询成功, 获取到 ${data.length} 条记录`);
      return data;
      
    } catch (error) {
      console.error('❌ 条件查询失败:', error.message);
      return null;
    }
  }

  // 更新持仓数量
  async updateHoldingQuantity(ticker, newQuantity) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from(this.tableName)
        .update({ holding: newQuantity })
        .eq('ticker', ticker)
        .select();

      if (error) throw error;
      
      if (data.length === 0) {
        console.log('⚠️ 未找到对应的持仓记录');
        return null;
      }
      
      console.log('✅ 持仓数量更新成功, 代码:', ticker, '新数量:', newQuantity);
      return data[0];
      
    } catch (error) {
      console.error('❌ 更新持仓数量失败:', error.message);
      return null;
    }
  }

  // 更新成本价
  async updateCostPerShare(ticker, newCost) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from(this.tableName)
        .update({ costPerShare: newCost })
        .eq('ticker', ticker)
        .select();

      if (error) throw error;
      
      console.log('✅ 成本价更新成功, 代码:', ticker, '新成本:', newCost);
      return data[0];
      
    } catch (error) {
      console.error('❌ 更新成本价失败:', error.message);
      return null;
    }
  }

  // 更新汇率并重新计算人民币成本
  async updateExchangeRate(newRate) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      // 首先获取所有 USD 持仓
      const { data: holdings, error: fetchError } = await this.supabase
        .from(this.tableName)
        .select('*')
        .eq('currency', 'USD');

      if (fetchError) throw fetchError;

      // 批量更新汇率和人民币成本
      const updates = holdings.map(holding => ({
        ticker: holding.ticker,
        exchangeRate: newRate,
        CostCNY: holding.costPerShare * newRate
      }));

      const { data, error } = await this.supabase
        .from(this.tableName)
        .upsert(updates)
        .select();

      if (error) throw error;
      
      console.log(`✅ 汇率更新成功, 新汇率: ${newRate}, 更新了 ${data.length} 条记录`);
      return data;
      
    } catch (error) {
      console.error('❌ 更新汇率失败:', error.message);
      return null;
    }
  }

  // 删除持仓记录
  async deleteHolding(ticker) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { error } = await this.supabase
        .from(this.tableName)
        .delete()
        .eq('ticker', ticker);

      if (error) throw error;
      
      console.log('✅ 持仓记录删除成功, 代码:', ticker);
      return true;
      
    } catch (error) {
      console.error('❌ 删除持仓记录失败:', error.message);
      return false;
    }
  }

  // 获取账户统计信息
  async getAccountStats() {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from(this.tableName)
        .select('*');

      if (error) throw error;

      const stats = {
        totalHoldings: data.length,
        totalUSDValue: data.reduce((sum, holding) => sum + (holding.costPerShare * holding.holding), 0),
        totalCNYValue: data.reduce((sum, holding) => sum + (holding.CostCNY * holding.holding), 0),
        holdingsByCurrency: data.reduce((acc, holding) => {
          acc[holding.currency] = (acc[holding.currency] || 0) + 1;
          return acc;
        }, {}),
        holdingsByType: data.reduce((acc, holding) => {
          acc[holding.quoteType] = (acc[holding.quoteType] || 0) + 1;
          return acc;
        }, {}),
        topHoldings: data
          .filter(h => h.holding > 0)
          .sort((a, b) => (b.costPerShare * b.holding) - (a.costPerShare * a.holding))
          .slice(0, 5)
          .map(h => ({
            ticker: h.ticker,
            value: h.costPerShare * h.holding
          }))
      };

      console.log('📊 账户统计信息:', stats);
      return stats;
      
    } catch (error) {
      console.error('❌ 获取统计信息失败:', error.message);
      return null;
    }
  }

  // 计算持仓总价值
  async calculatePortfolioValue() {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from(this.tableName)
        .select('ticker, holding, costPerShare, currency, CostCNY');

      if (error) throw error;

      const portfolio = {
        totalUSD: 0,
        totalCNY: 0,
        byCurrency: {},
        byType: {}
      };

      data.forEach(holding => {
        const usdValue = holding.costPerShare * holding.holding;
        const cnyValue = holding.CostCNY * holding.holding;
        
        portfolio.totalUSD += usdValue;
        portfolio.totalCNY += cnyValue;
        
        // 按货币统计
        portfolio.byCurrency[holding.currency] = (portfolio.byCurrency[holding.currency] || 0) + usdValue;
      });

      console.log('💰 投资组合价值:');
      console.log(`   总价值 (USD): $${portfolio.totalUSD.toFixed(2)}`);
      console.log(`   总价值 (CNY): ¥${portfolio.totalCNY.toFixed(2)}`);
      Object.entries(portfolio.byCurrency).forEach(([currency, value]) => {
        console.log(`   ${currency}: $${value.toFixed(2)}`);
      });

      return portfolio;
      
    } catch (error) {
      console.error('❌ 计算投资组合价值失败:', error.message);
      return null;
    }
  }
}

// 使用示例
async function runDemo() {
  const manager = new AccountIB7075Manager();
  
  // 1. 初始化
  const initialized = await manager.initialize();
  if (!initialized) return;

  console.log('\n' + '='.repeat(50));
  console.log('🚀 开始演示 IB7075 账户操作');
  console.log('='.repeat(50) + '\n');

  // 2. 创建或更新单条持仓记录
  console.log('1. 创建/更新单条持仓记录...');
  const newHolding = await manager.upsertHolding({
    ticker: 'AAPL',
    company: 'Apple Inc.',
    holding: 100,
    costPerShare: 150.50,
    currency: 'USD',
    accountID: 'IB7075',
    quoteType: 'equity',
    exchange: 'US',
    CostCNY: 1053.50,
    exchangeRate: 7.0
  });

  // 3. 批量创建记录
  console.log('\n2. 批量创建持仓记录...');
  const batchHoldings = await manager.upsertMultipleHoldings([
    {
      ticker: 'GOOGL',
      company: 'Alphabet Inc.',
      holding: 50,
      costPerShare: 2800.75,
      currency: 'USD',
      accountID: 'IB7075',
      quoteType: 'equity',
      exchange: 'US',
      CostCNY: 19605.25,
      exchangeRate: 7.0
    },
    {
      ticker: 'TSLA',
      company: 'Tesla Inc.',
      holding: 25,
      costPerShare: 250.30,
      currency: 'USD',
      accountID: 'IB7075',
      quoteType: 'equity',
      exchange: 'US',
      CostCNY: 1752.10,
      exchangeRate: 7.0
    },
    {
      ticker: '700.HK',
      company: 'Tencent Holdings',
      holding: 200,
      costPerShare: 350.00,
      currency: 'HKD',
      accountID: 'IB7075',
      quoteType: 'equity',
      exchange: 'HK',
      CostCNY: 315.00,
      exchangeRate: 0.9
    }
  ]);

  // 4. 读取所有持仓记录
  console.log('\n3. 读取所有持仓记录...');
  const allHoldings = await manager.getAllHoldings();
  if (allHoldings) {
    allHoldings.forEach(holding => {
      const totalValue = holding.costPerShare * holding.holding;
      console.log(`   ${holding.ticker}: ${holding.holding}股 @ $${holding.costPerShare} = $${totalValue.toFixed(2)}`);
    });
  }

  // 5. 查询特定持仓
  console.log('\n4. 查询特定持仓...');
  const aaplHolding = await manager.getHoldingByTicker('AAPL');
  if (aaplHolding) {
    console.log(`   AAPL 持仓: ${aaplHolding.holding}股, 成本: $${aaplHolding.costPerShare}`);
  }

  // 6. 更新持仓数量
  console.log('\n5. 更新持仓数量...');
  await manager.updateHoldingQuantity('AAPL', 150);

  // 7. 更新成本价
  console.log('\n6. 更新成本价...');
  await manager.updateCostPerShare('AAPL', 155.25);

  // 8. 更新汇率
  console.log('\n7. 更新汇率...');
  await manager.updateExchangeRate(7.2);

  // 9. 获取统计信息
  console.log('\n8. 获取账户统计信息...');
  await manager.getAccountStats();

  // 10. 计算投资组合价值
  console.log('\n9. 计算投资组合价值...');
  await manager.calculatePortfolioValue();

  // 11. 条件查询
  console.log('\n10. 条件查询 - USD 持仓...');
  const usdHoldings = await manager.getHoldingsByCondition({
    currency: 'USD',
    minHolding: 1
  });

  console.log('\n' + '='.repeat(50));
  console.log('🎉 IB7075 账户演示完成!');
  console.log('='.repeat(50));
}

// 运行演示
if (require.main === module) {
  runDemo().catch(console.error);
}

// 导出类供其他模块使用
module.exports = AccountIB7075Manager;
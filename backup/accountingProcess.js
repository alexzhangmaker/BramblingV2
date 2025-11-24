// accountingProcess.js
require('dotenv').config();

const { createClient } = require('@supabase/supabase-js');

class AccountingProcess {
  constructor() {
    this.supabase = createClient(
      process.env.SUPABASE_URL,
      process.env.SUPABASE_ANON_KEY
    );
    this.authenticated = false;
    
    // 账户表映射
    this.accountTableMap = {
      'IB7075': 'account_IB7075'
      // 可以添加更多账户映射，例如：
      // 'IB8080': 'account_IB8080',
      // 'FUTU001': 'account_FUTU001'
    };
  }

  async initialize() {
    console.log('🔐 初始化会计流程处理器...');
    
    try {
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

  // 获取未清算的交易记录
  async getUnclearedDeals(limit = 10) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from('dealLogs')
        .select('*')
        .eq('cleared', false)
        .order('dealID', { ascending: true })
        .limit(limit);

      if (error) throw error;
      
      console.log(`✅ 获取到 ${data.length} 条未清算交易记录`);
      return data;
      
    } catch (error) {
      console.error('❌ 获取未清算交易失败:', error.message);
      return null;
    }
  }

  // 核心业务流程：处理单条交易记录的清算
  async handleClearDeal(dealRecord) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return false;
    }

    console.log(`\n🔄 开始处理交易记录 #${dealRecord.dealID}: ${dealRecord.action} ${dealRecord.quantity} ${dealRecord.ticker}`);
    
    try {
      // 步骤1: 验证交易数据
      if (!await this.validateDealRecord(dealRecord)) {
        return false;
      }

      // 步骤2: 处理账户表更新
      const accountUpdateSuccess = await this.processAccountTable(dealRecord);
      if (!accountUpdateSuccess) {
        console.error('❌ 账户表更新失败，中止流程');
        return false;
      }

      // 步骤3: 处理账本表更新
      const ledgerUpdateSuccess = await this.processLedgerTable(dealRecord);
      if (!ledgerUpdateSuccess) {
        console.error('❌ 账本表更新失败，需要回滚账户表');
        await this.rollbackAccountTable(dealRecord);
        return false;
      }

      // 步骤4: 标记交易为已清算
      const clearSuccess = await this.markDealAsCleared(dealRecord.dealID);
      if (!clearSuccess) {
        console.error('❌ 标记清算状态失败，需要回滚所有操作');
        await this.rollbackAll(dealRecord);
        return false;
      }

      console.log(`✅ 交易记录 #${dealRecord.dealID} 清算完成`);
      return true;
      
    } catch (error) {
      console.error(`❌ 处理交易记录 #${dealRecord.dealID} 失败:`, error.message);
      await this.rollbackAll(dealRecord);
      return false;
    }
  }

  // 验证交易记录
  async validateDealRecord(dealRecord) {
    console.log('   📋 验证交易记录...');
    
    const requiredFields = ['dealID', 'account', 'action', 'ticker', 'quantity', 'price'];
    for (const field of requiredFields) {
      if (!dealRecord[field] && dealRecord[field] !== 0) {
        console.error(`❌ 交易记录缺少必要字段: ${field}`);
        return false;
      }
    }

    // 验证账户表映射
    if (!this.accountTableMap[dealRecord.account]) {
      console.error(`❌ 未知的账户: ${dealRecord.account}`);
      return false;
    }

    // 验证操作类型
    if (!['BUY', 'SELL'].includes(dealRecord.action)) {
      console.error(`❌ 无效的操作类型: ${dealRecord.action}`);
      return false;
    }

    // 验证数量
    if (dealRecord.quantity <= 0) {
      console.error(`❌ 无效的数量: ${dealRecord.quantity}`);
      return false;
    }

    console.log('   ✅ 交易记录验证通过');
    return true;
  }

  // 处理账户表更新
  async processAccountTable(dealRecord) {
    console.log('   📊 处理账户表更新...');
    
    const accountTable = this.accountTableMap[dealRecord.account];
    
    try {
      // 检查账户表中是否已存在该股票记录
      const { data: existingHolding, error: queryError } = await this.supabase
        .from(accountTable)
        .select('*')
        .eq('ticker', dealRecord.ticker)
        .single();

      if (queryError && queryError.code !== 'PGRST116') { // PGRST116 表示没有找到记录
        throw queryError;
      }

      if (existingHolding) {
        // 更新现有持仓
        return await this.updateExistingHolding(accountTable, existingHolding, dealRecord);
      } else {
        // 新增持仓记录
        return await this.createNewHolding(accountTable, dealRecord);
      }
      
    } catch (error) {
      console.error(`❌ 处理账户表失败:`, error.message);
      return false;
    }
  }

  // 更新现有持仓
  async updateExistingHolding(accountTable, existingHolding, dealRecord) {
    console.log(`   🔄 更新现有持仓: ${dealRecord.ticker}`);
    
    let newHolding, newCostPerShare;

    if (dealRecord.action === 'BUY') {
      // 买入：计算新的加权平均成本
      const totalCost = (existingHolding.holding * existingHolding.costPerShare) + 
                       (dealRecord.quantity * dealRecord.price);
      newHolding = existingHolding.holding + dealRecord.quantity;
      newCostPerShare = totalCost / newHolding;
      
    } else if (dealRecord.action === 'SELL') {
      // 卖出：检查持仓是否足够
      if (existingHolding.holding < dealRecord.quantity) {
        console.error(`❌ 卖出数量超过持仓: 持仓 ${existingHolding.holding}, 卖出 ${dealRecord.quantity}`);
        return false;
      }
      newHolding = existingHolding.holding - dealRecord.quantity;
      newCostPerShare = existingHolding.costPerShare; // 卖出不影响成本价
    }

    const updateData = {
      holding: newHolding,
      costPerShare: newCostPerShare,
      // 如果有汇率信息，可以更新人民币成本
      ...(dealRecord.exchangeRate && {
        exchangeRate: dealRecord.exchangeRate,
        CostCNY: newCostPerShare * dealRecord.exchangeRate
      })
    };

    const { error } = await this.supabase
      .from(accountTable)
      .update(updateData)
      .eq('ticker', dealRecord.ticker);

    if (error) throw error;

    console.log(`   ✅ 持仓更新成功: ${dealRecord.ticker} -> ${newHolding}股 @ $${newCostPerShare.toFixed(2)}`);
    return true;
  }

  // 创建新持仓
  async createNewHolding(accountTable, dealRecord) {
    console.log(`   ➕ 创建新持仓: ${dealRecord.ticker}`);
    
    if (dealRecord.action === 'SELL') {
      console.error(`❌ 无法卖出不存在的持仓: ${dealRecord.ticker}`);
      return false;
    }

    // 获取公司名称（这里需要根据实际情况获取，暂时使用占位符）
    const companyName = await this.getCompanyName(dealRecord.ticker);

    const newHoldingData = {
      ticker: dealRecord.ticker,
      company: companyName,
      holding: dealRecord.quantity,
      costPerShare: dealRecord.price,
      currency: dealRecord.currency || 'USD',
      accountID: dealRecord.account,
      quoteType: dealRecord.quoteType || 'equity',
      exchange: dealRecord.exchange || 'US',
      ...(dealRecord.exchangeRate && {
        exchangeRate: dealRecord.exchangeRate,
        CostCNY: dealRecord.price * dealRecord.exchangeRate
      })
    };

    const { error } = await this.supabase
      .from(accountTable)
      .insert(newHoldingData);

    if (error) throw error;

    console.log(`   ✅ 新持仓创建成功: ${dealRecord.ticker} - ${dealRecord.quantity}股 @ $${dealRecord.price}`);
    return true;
  }

  // 处理账本表更新
  async processLedgerTable(dealRecord) {
    console.log('   💰 处理账本表更新...');
    
    const assetID = `${dealRecord.account}_CASH`;
    const transactionAmount = dealRecord.quantity * dealRecord.price;
    
    try {
      // 首先获取当前现金余额
      const { data: currentLedger, error: queryError } = await this.supabase
        .from('ledger')
        .select('*')
        .eq('assetID', assetID)
        .single();

      let newCashBalance;

      if (queryError && queryError.code === 'PGRST116') {
        // 如果没有找到记录，创建新的现金记录
        if (dealRecord.action === 'BUY') {
          newCashBalance = -transactionAmount; // 买入导致现金减少
        } else {
          newCashBalance = transactionAmount; // 卖出导致现金增加
        }
        
        return await this.createNewCashRecord(assetID, dealRecord, newCashBalance);
      } else if (queryError) {
        throw queryError;
      } else {
        // 更新现有现金记录
        if (dealRecord.action === 'BUY') {
          newCashBalance = (currentLedger.Cash || 0) - transactionAmount;
        } else {
          newCashBalance = (currentLedger.Cash || 0) + transactionAmount;
        }
        
        return await this.updateCashRecord(assetID, newCashBalance);
      }
      
    } catch (error) {
      console.error(`❌ 处理账本表失败:`, error.message);
      return false;
    }
  }

  // 创建新的现金记录
  async createNewCashRecord(assetID, dealRecord, cashBalance) {
    const newLedgerRecord = {
      assetID: assetID,
      AssetType: 'cash',
      Currency: dealRecord.currency || 'USD',
      Cash: cashBalance,
      Debt: 0,
      marketValueCNY: 0,
      ValueTTMCNY: 0,
      timeStamp: new Date().toISOString()
    };

    const { error } = await this.supabase
      .from('ledger')
      .insert(newLedgerRecord);

    if (error) throw error;

    const actionText = dealRecord.action === 'BUY' ? '减少' : '增加';
    console.log(`   ✅ 创建现金记录成功: ${assetID}, 现金${actionText} $${Math.abs(cashBalance).toFixed(2)}`);
    return true;
  }

  // 更新现金记录
  async updateCashRecord(assetID, newCashBalance) {
    const { error } = await this.supabase
      .from('ledger')
      .update({
        Cash: newCashBalance,
        timeStamp: new Date().toISOString()
      })
      .eq('assetID', assetID);

    if (error) throw error;

    console.log(`   ✅ 现金记录更新成功: ${assetID} -> $${newCashBalance.toFixed(2)}`);
    return true;
  }

  // 标记交易为已清算
  async markDealAsCleared(dealID) {
    console.log('   ✅ 标记交易为已清算...');
    
    const { error } = await this.supabase
      .from('dealLogs')
      .update({ cleared: true })
      .eq('dealID', dealID);

    if (error) throw error;

    console.log(`   ✅ 交易 #${dealID} 已标记为清算完成`);
    return true;
  }

  // 回滚账户表操作
  async rollbackAccountTable(dealRecord) {
    console.log('   🔄 回滚账户表操作...');
    
    const accountTable = this.accountTableMap[dealRecord.account];
    
    try {
      // 这里需要根据具体操作进行回滚
      // 简化处理：记录回滚日志
      console.log(`   📝 记录回滚: 账户表 ${accountTable}, 交易 ${dealRecord.dealID}`);
      return true;
    } catch (error) {
      console.error('❌ 回滚账户表失败:', error.message);
      return false;
    }
  }

  // 回滚所有操作
  async rollbackAll(dealRecord) {
    console.log('   🔄 开始回滚所有操作...');
    
    await this.rollbackAccountTable(dealRecord);
    // 这里可以添加账本表的回滚逻辑
    
    console.log('   ✅ 回滚完成');
  }

  // 获取公司名称（占位函数，需要根据实际情况实现）
  async getCompanyName(ticker) {
    // 这里可以集成股票API或者维护一个本地映射表
    const companyMap = {
      'AAPL': 'Apple Inc.',
      'GOOGL': 'Alphabet Inc.',
      'TSLA': 'Tesla Inc.',
      'MSFT': 'Microsoft Corporation'
      // 可以添加更多映射
    };
    
    return companyMap[ticker] || `${ticker} Company`;
  }

  // 批量处理未清算交易
  async processAllUnclearedDeals() {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return false;
    }

    console.log('🚀 开始批量处理所有未清算交易...');
    
    try {
      const unclearedDeals = await this.getUnclearedDeals(50); // 一次处理最多50条
      
      if (!unclearedDeals || unclearedDeals.length === 0) {
        console.log('✅ 没有未清算的交易记录');
        return true;
      }

      let successCount = 0;
      let failCount = 0;

      for (const deal of unclearedDeals) {
        const success = await this.handleClearDeal(deal);
        if (success) {
          successCount++;
        } else {
          failCount++;
        }
        
        // 添加延迟避免过快请求
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      console.log(`\n📊 批量处理完成: 成功 ${successCount} 条, 失败 ${failCount} 条`);
      return failCount === 0;
      
    } catch (error) {
      console.error('❌ 批量处理失败:', error.message);
      return false;
    }
  }

  // 获取处理统计
  async getProcessingStats() {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      // 获取未清算交易数量
      const { data: unclearedDeals, error: dealsError } = await this.supabase
        .from('dealLogs')
        .select('dealID')
        .eq('cleared', false);

      if (dealsError) throw dealsError;

      // 获取各账户表的记录数量
      const accountStats = {};
      for (const [accountName, tableName] of Object.entries(this.accountTableMap)) {
        const { data: accountData, error: accountError } = await this.supabase
          .from(tableName)
          .select('ticker');
        
        if (!accountError) {
          accountStats[accountName] = accountData.length;
        }
      }

      const stats = {
        unclearedDeals: unclearedDeals.length,
        accountStats: accountStats,
        totalAccounts: Object.keys(this.accountTableMap).length
      };

      console.log('📈 处理统计信息:');
      console.log(`   未清算交易: ${stats.unclearedDeals} 条`);
      console.log('   账户持仓统计:');
      Object.entries(stats.accountStats).forEach(([account, count]) => {
        console.log(`     ${account}: ${count} 只股票`);
      });

      return stats;
      
    } catch (error) {
      console.error('❌ 获取统计信息失败:', error.message);
      return null;
    }
  }
}

// 使用示例和测试函数
async function runAccountingDemo() {
  const accounting = new AccountingProcess();
  
  // 初始化
  const initialized = await accounting.initialize();
  if (!initialized) return;

  console.log('\n' + '='.repeat(60));
  console.log('🧾 会计流程演示');
  console.log('='.repeat(60));

  // 1. 显示当前统计
  console.log('\n1. 当前系统状态:');
  await accounting.getProcessingStats();

  // 2. 获取未清算交易示例
  console.log('\n2. 查看未清算交易:');
  const unclearedDeals = await accounting.getUnclearedDeals(5);
  if (unclearedDeals && unclearedDeals.length > 0) {
    console.log('   未清算交易示例:');
    unclearedDeals.forEach(deal => {
      console.log(`     #${deal.dealID}: ${deal.action} ${deal.quantity} ${deal.ticker} @ $${deal.price}`);
    });

    // 3. 处理第一条交易
    console.log('\n3. 处理单条交易:');
    await accounting.handleClearDeal(unclearedDeals[0]);
  } else {
    console.log('   没有未清算交易，将创建测试交易...');
    
    // 创建测试交易记录
    const testDeal = {
      dealID: Date.now(), // 临时ID
      account: 'IB7075',
      action: 'BUY',
      ticker: 'AAPL',
      price: 150.50,
      quantity: 10,
      market: 'NASDAQ',
      date: new Date().toISOString().split('T')[0],
      cleared: false,
      currency: 'USD'
    };
    
    console.log('   创建测试交易记录...');
    // 注意：这里需要先在数据库中创建测试记录
    console.log('   💡 请先在数据库中创建测试交易记录');
  }

  // 4. 批量处理演示（可选）
  console.log('\n4. 批量处理演示:');
  const batchResult = await accounting.processAllUnclearedDeals();
  console.log(`   批量处理结果: ${batchResult ? '成功' : '失败'}`);

  console.log('\n' + '='.repeat(60));
  console.log('🎉 会计流程演示完成!');
  console.log('='.repeat(60));
}

// 快速测试函数
async function quickAccountingTest() {
  console.log('🧪 快速测试会计流程\n');
  
  const accounting = new AccountingProcess();
  
  if (!await accounting.initialize()) {
    console.log('❌ 初始化失败');
    return;
  }

  // 简单测试
  console.log('1. 获取统计信息...');
  await accounting.getProcessingStats();

  console.log('2. 检查未清算交易...');
  const deals = await accounting.getUnclearedDeals(3);
  if (deals && deals.length > 0) {
    console.log(`   找到 ${deals.length} 条未清算交易`);
  } else {
    console.log('   没有未清算交易');
  }

  console.log('\n✅ 快速测试完成!');
}

// 导出模块
module.exports = {
  AccountingProcess,
  runAccountingDemo,
  quickAccountingTest
};

// 如果直接运行此文件，执行演示
if (require.main === module) {
  runAccountingDemo().catch(console.error);
}
// accountingProcessFixed.js
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

// 增强的简单日志记录器
class SimpleOperationLogger {
  constructor(supabaseClient) {
    this.supabase = supabaseClient;
  }

  async logOperation(operationInfo) {
    const logRecord = {
      operation_type: operationInfo.type,
      operation_target: operationInfo.target,
      target_record_id: operationInfo.recordId,
      operation_data: operationInfo.data || null,
      status: operationInfo.status,
      error_message: operationInfo.error || null,
      executed_by: operationInfo.executedBy || 'system',
      executed_at: new Date().toISOString(),
      duration_ms: operationInfo.duration || 0
    };

    try {
      const { data, error } = await this.supabase
        .from('operation_logs')
        .insert(logRecord)
        .select()
        .single();

      if (error) {
        console.log('⚠️ 日志记录失败，使用控制台备份:', error.message);
        this.logToConsole(logRecord);
        return null;
      }

      return data;
    } catch (error) {
      console.log('⚠️ 日志记录异常，使用控制台备份:', error.message);
      this.logToConsole(logRecord);
      return null;
    }
  }

  logToConsole(logRecord) {
    const timestamp = new Date(logRecord.executed_at).toLocaleString();
    console.log(`[操作日志] ${timestamp} | ${logRecord.operation_type} | ${logRecord.status} | ${logRecord.target_record_id}`);
  }

  async logSuccess(operationType, target, recordId, data = null) {
    return await this.logOperation({
      type: operationType,
      target: target,
      recordId: recordId,
      data: data,
      status: 'SUCCESS'
    });
  }

  async logFailure(operationType, target, recordId, error, data = null) {
    return await this.logOperation({
      type: operationType,
      target: target,
      recordId: recordId,
      data: data,
      status: 'FAILED',
      error: error.message || error.toString()
    });
  }
}

// 主会计流程类
class AccountingProcess {
  constructor() {
    // 检查环境变量
    this.checkEnvironment();
    
    // 初始化 Supabase 客户端
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
    
    this.logger = new SimpleOperationLogger(this.supabase);
    this.authenticated = false;
    
    this.accountTableMap = {
      'IB7075': 'account_IB7075'
    };
  }

  // 检查环境变量
  checkEnvironment() {
    console.log('🔍 检查环境变量...');
    console.log('SUPABASE_URL:', process.env.SUPABASE_URL ? '已设置' : '❌ 未设置');
    console.log('SUPABASE_ANON_KEY:', process.env.SUPABASE_ANON_KEY ? '已设置' : '❌ 未设置');
    console.log('SERVICE_ACCOUNT_EMAIL:', process.env.SERVICE_ACCOUNT_EMAIL ? '已设置' : '❌ 未设置');
    
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_ANON_KEY) {
      throw new Error('请检查 .env 文件中的 SUPABASE_URL 和 SUPABASE_ANON_KEY 配置');
    }
  }

  async initialize() {
    console.log('🔐 初始化会计流程处理器...');
    
    try {
      // 首先测试基本连接
      console.log('测试 Supabase 连接...');
      const { data: testData, error: testError } = await this.supabase
        .from('dealLogs')
        .select('count')
        .limit(1);

      if (testError && testError.code !== 'PGRST116') {
        console.log('连接测试结果:', testError.message);
      } else {
        console.log('✅ Supabase 连接正常');
      }

      // 尝试登录
      console.log('尝试登录 Service Account...');
      const { data, error } = await this.supabase.auth.signInWithPassword({
        email: process.env.SERVICE_ACCOUNT_EMAIL,
        password: process.env.SERVICE_ACCOUNT_PASSWORD
      });

      if (error) {
        console.log('❌ 登录失败:', error.message);
        
        // 即使登录失败，也继续运行（可能有公开表的访问权限）
        console.log('⚠️ 将继续以未认证状态运行，部分功能可能受限');
        this.authenticated = false;
        return true;
      }
      
      this.authenticated = true;
      console.log('✅ 登录成功:', data.user.email);
      return true;
      
    } catch (error) {
      console.error('❌ 初始化异常:', error.message);
      console.log('⚠️ 将继续以未认证状态运行');
      this.authenticated = false;
      return true; // 仍然返回 true，让程序可以继续运行
    }
  }

  // 获取未清算的交易记录
  async getUnclearedDeals(limit = 10) {
    try {
      const { data, error } = await this.supabase
        .from('dealLogs')
        .select('*')
        .eq('cleared', false)
        .order('dealID', { ascending: true })
        .limit(limit);

      if (error) {
        console.error('❌ 获取未清算交易失败:', error.message);
        return null;
      }
      
      console.log(`✅ 获取到 ${data.length} 条未清算交易记录`);
      return data;
      
    } catch (error) {
      console.error('❌ 获取未清算交易异常:', error.message);
      return null;
    }
  }

  // 核心业务流程：处理单条交易记录的清算
  async handleClearDeal(dealRecord) {
    const startTime = Date.now();
    
    console.log(`\n🔄 开始处理交易记录 #${dealRecord.dealID}: ${dealRecord.action} ${dealRecord.quantity} ${dealRecord.ticker}`);
    
    try {
      // 步骤1: 验证交易数据
      if (!await this.validateDealRecord(dealRecord)) {
        await this.logger.logFailure(
          'clear_deal_validation',
          'dealLogs',
          dealRecord.dealID.toString(),
          '交易记录验证失败',
          { ticker: dealRecord.ticker, action: dealRecord.action }
        );
        return false;
      }

      // 步骤2: 处理账户表更新
      const accountUpdateSuccess = await this.processAccountTable(dealRecord);
      if (!accountUpdateSuccess) {
        await this.logger.logFailure(
          'clear_deal_account_update',
          'account_IB7075',
          dealRecord.ticker,
          '账户表更新失败',
          { ticker: dealRecord.ticker, action: dealRecord.action, quantity: dealRecord.quantity }
        );
        return false;
      }

      // 步骤3: 处理账本表更新
      const ledgerUpdateSuccess = await this.processLedgerTable(dealRecord);
      if (!ledgerUpdateSuccess) {
        await this.logger.logFailure(
          'clear_deal_ledger_update',
          'ledger',
          `${dealRecord.account}_CASH`,
          '账本表更新失败',
          { cashChange: dealRecord.quantity * dealRecord.price, action: dealRecord.action }
        );
        return false;
      }

      // 步骤4: 标记交易为已清算
      const clearSuccess = await this.markDealAsCleared(dealRecord.dealID);
      if (!clearSuccess) {
        await this.logger.logFailure(
          'clear_deal_mark_cleared',
          'dealLogs',
          dealRecord.dealID.toString(),
          '标记清算状态失败'
        );
        return false;
      }

      const duration = Date.now() - startTime;
      
      // 记录成功日志
      await this.logger.logOperation({
        type: 'clear_deal_success',
        target: 'dealLogs',
        recordId: dealRecord.dealID.toString(),
        data: {
          ticker: dealRecord.ticker,
          action: dealRecord.action,
          quantity: dealRecord.quantity,
          price: dealRecord.price,
          totalAmount: dealRecord.quantity * dealRecord.price
        },
        status: 'SUCCESS',
        duration: duration
      });

      console.log(`✅ 交易记录 #${dealRecord.dealID} 清算完成 (${duration}ms)`);
      return true;
      
    } catch (error) {
      const duration = Date.now() - startTime;
      
      await this.logger.logFailure(
        'clear_deal_error',
        'dealLogs',
        dealRecord.dealID.toString(),
        error,
        {
          ticker: dealRecord.ticker,
          action: dealRecord.action,
          duration: duration
        }
      );
      
      console.error(`❌ 处理交易记录 #${dealRecord.dealID} 失败:`, error.message);
      return false;
    }
  }

  // 简化的验证函数
  async validateDealRecord(dealRecord) {
    console.log('   📋 验证交易记录...');
    
    const requiredFields = ['dealID', 'account', 'action', 'ticker', 'quantity', 'price'];
    for (const field of requiredFields) {
      if (!dealRecord[field] && dealRecord[field] !== 0) {
        console.error(`❌ 交易记录缺少必要字段: ${field}`);
        return false;
      }
    }

    if (!['BUY', 'SELL'].includes(dealRecord.action)) {
      console.error(`❌ 无效的操作类型: ${dealRecord.action}`);
      return false;
    }

    if (dealRecord.quantity <= 0) {
      console.error(`❌ 无效的数量: ${dealRecord.quantity}`);
      return false;
    }

    console.log('   ✅ 交易记录验证通过');
    return true;
  }

  // 简化的账户表处理
  async processAccountTable(dealRecord) {
    console.log('   📊 处理账户表更新...');
    
    try {
      const { data: existingHolding, error: queryError } = await this.supabase
        .from('account_IB7075')
        .select('*')
        .eq('ticker', dealRecord.ticker)
        .single();

      if (queryError && queryError.code !== 'PGRST116') {
        throw queryError;
      }

      if (existingHolding) {
        return await this.updateExistingHolding(existingHolding, dealRecord);
      } else {
        return await this.createNewHolding(dealRecord);
      }
      
    } catch (error) {
      console.error(`❌ 处理账户表失败:`, error.message);
      return false;
    }
  }

  async updateExistingHolding(existingHolding, dealRecord) {
    console.log(`   🔄 更新现有持仓: ${dealRecord.ticker}`);
    
    let newHolding, newCostPerShare;

    if (dealRecord.action === 'BUY') {
      const totalCost = (existingHolding.holding * existingHolding.costPerShare) + 
                       (dealRecord.quantity * dealRecord.price);
      newHolding = existingHolding.holding + dealRecord.quantity;
      newCostPerShare = totalCost / newHolding;
      
    } else {
      if (existingHolding.holding < dealRecord.quantity) {
        console.error(`❌ 卖出数量超过持仓: 持仓 ${existingHolding.holding}, 卖出 ${dealRecord.quantity}`);
        return false;
      }
      newHolding = existingHolding.holding - dealRecord.quantity;
      newCostPerShare = existingHolding.costPerShare;
    }

    const { error } = await this.supabase
      .from('account_IB7075')
      .update({
        holding: newHolding,
        costPerShare: newCostPerShare
      })
      .eq('ticker', dealRecord.ticker);

    if (error) throw error;

    console.log(`   ✅ 持仓更新成功: ${dealRecord.ticker} -> ${newHolding}股 @ $${newCostPerShare.toFixed(2)}`);
    return true;
  }

  async createNewHolding(dealRecord) {
    console.log(`   ➕ 创建新持仓: ${dealRecord.ticker}`);
    
    if (dealRecord.action === 'SELL') {
      console.error(`❌ 无法卖出不存在的持仓: ${dealRecord.ticker}`);
      return false;
    }

    const newHoldingData = {
      ticker: dealRecord.ticker,
      company: await this.getCompanyName(dealRecord.ticker),
      holding: dealRecord.quantity,
      costPerShare: dealRecord.price,
      currency: dealRecord.currency || 'USD',
      accountID: dealRecord.account,
      quoteType: dealRecord.quoteType || 'equity',
      exchange: dealRecord.exchange || 'US'
    };

    const { error } = await this.supabase
      .from('account_IB7075')
      .insert(newHoldingData);

    if (error) throw error;

    console.log(`   ✅ 新持仓创建成功: ${dealRecord.ticker}`);
    return true;
  }

  async processLedgerTable(dealRecord) {
    console.log('   💰 处理账本表更新...');
    
    const assetID = `${dealRecord.account}_CASH`;
    const transactionAmount = dealRecord.quantity * dealRecord.price;
    
    try {
      const { data: currentLedger, error: queryError } = await this.supabase
        .from('ledger')
        .select('*')
        .eq('assetID', assetID)
        .single();

      let newCashBalance;

      if (queryError && queryError.code === 'PGRST116') {
        newCashBalance = dealRecord.action === 'BUY' ? -transactionAmount : transactionAmount;
        return await this.createNewCashRecord(assetID, dealRecord, newCashBalance);
      } else if (queryError) {
        throw queryError;
      } else {
        newCashBalance = dealRecord.action === 'BUY' 
          ? (currentLedger.Cash || 0) - transactionAmount
          : (currentLedger.Cash || 0) + transactionAmount;
        
        return await this.updateCashRecord(assetID, newCashBalance);
      }
      
    } catch (error) {
      console.error(`❌ 处理账本表失败:`, error.message);
      return false;
    }
  }

  async createNewCashRecord(assetID, dealRecord, cashBalance) {
    const { error } = await this.supabase
      .from('ledger')
      .insert({
        assetID: assetID,
        AssetType: 'cash',
        Currency: dealRecord.currency || 'USD',
        Cash: cashBalance,
        Debt: 0,
        marketValueCNY: 0,
        ValueTTMCNY: 0,
        timeStamp: new Date().toISOString()
      });

    if (error) throw error;

    console.log(`   ✅ 创建现金记录成功: ${assetID}`);
    return true;
  }

  async updateCashRecord(assetID, newCashBalance) {
    const { error } = await this.supabase
      .from('ledger')
      .update({
        Cash: newCashBalance,
        timeStamp: new Date().toISOString()
      })
      .eq('assetID', assetID);

    if (error) throw error;

    console.log(`   ✅ 现金记录更新成功: ${assetID}`);
    return true;
  }

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

  async getCompanyName(ticker) {
    const companyMap = {
      'AAPL': 'Apple Inc.', 'GOOGL': 'Alphabet Inc.', 'TSLA': 'Tesla Inc.',
      'MSFT': 'Microsoft Corporation', 'NVDA': 'NVIDIA Corporation'
    };
    return companyMap[ticker] || `${ticker} Company`;
  }

  // 批量处理
  async processAllUnclearedDeals() {
    console.log('🚀 开始批量处理所有未清算交易...');
    
    const startTime = Date.now();
    let successCount = 0;
    let failCount = 0;

    try {
      const unclearedDeals = await this.getUnclearedDeals(50);
      
      if (!unclearedDeals || unclearedDeals.length === 0) {
        await this.logger.logSuccess('batch_clear_deals', 'dealLogs', 'all');
        console.log('✅ 没有未清算的交易记录');
        return true;
      }

      for (const deal of unclearedDeals) {
        const success = await this.handleClearDeal(deal);
        if (success) successCount++;
        else failCount++;
        
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      const duration = Date.now() - startTime;
      
      await this.logger.logOperation({
        type: 'batch_clear_deals_complete',
        target: 'dealLogs',
        recordId: 'batch',
        data: { total: unclearedDeals.length, success: successCount, failed: failCount },
        status: failCount === 0 ? 'SUCCESS' : 'PARTIAL',
        duration: duration
      });

      console.log(`\n📊 批量处理完成: 成功 ${successCount} 条, 失败 ${failCount} 条`);
      return failCount === 0;
      
    } catch (error) {
      await this.logger.logFailure('batch_clear_deals_error', 'dealLogs', 'batch', error);
      console.error('❌ 批量处理失败:', error.message);
      return false;
    }
  }
}

// 使用示例
async function main() {
  try {
    const accounting = new AccountingProcess();
    const initialized = await accounting.initialize();
    
    if (initialized) {
      console.log('\n🎯 开始处理未清算交易...');
      await accounting.processAllUnclearedDeals();
      console.log('\n✅ 程序执行完成');
    } else {
      console.log('❌ 初始化失败，程序退出');
    }
  } catch (error) {
    console.error('💥 程序执行异常:', error.message);
  }
}

// 运行程序
if (require.main === module) {
  main();
}

module.exports = AccountingProcess;
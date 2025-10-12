// accountingProcessWithLog.js
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

// 内置的简单日志记录器（保持不变）
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

// 增强的会计流程类 - 支持多账户表
class AccountingProcess {
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
    
    this.logger = new SimpleOperationLogger(this.supabase);
    this.authenticated = false;
    
    // 动态账户表映射 - 支持多个账户
    this.accountTableMap = {
      'IB7075': 'account_IB7075',
      'IB1279': 'account_IB1279',
      'IB3979': 'account_IB3979',
      'IB6325': 'account_IB6325',
      'HTZQ': 'account_HTZQ',
      'GJZQ': 'account_GJZQ'
      // 可以继续添加更多账户映射
      // 'FUTU001': 'account_FUTU001',
      // 'WEBULL002': 'account_WEBULL002'
    };
  }

  // 检查环境变量（保持不变）
  checkEnvironment() {
    console.log('🔍 检查环境变量...');
    console.log('SUPABASE_URL:', process.env.SUPABASE_URL ? '已设置' : '❌ 未设置');
    console.log('SUPABASE_ANON_KEY:', process.env.SUPABASE_ANON_KEY ? '已设置' : '❌ 未设置');
    console.log('SERVICE_ACCOUNT_EMAIL:', process.env.SERVICE_ACCOUNT_EMAIL ? '已设置' : '❌ 未设置');
    
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_ANON_KEY) {
      throw new Error('请检查 .env 文件中的 SUPABASE_URL 和 SUPABASE_ANON_KEY 配置');
    }
  }

  // 初始化（保持不变）
  async initialize() {
    console.log('🔐 初始化会计流程处理器...');
    
    try {
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

      console.log('尝试登录 Service Account...');
      const { data, error } = await this.supabase.auth.signInWithPassword({
        email: process.env.SERVICE_ACCOUNT_EMAIL,
        password: process.env.SERVICE_ACCOUNT_PASSWORD
      });

      if (error) {
        console.log('❌ 登录失败:', error.message);
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
      return true;
    }
  }

  // 获取账户表名（新增方法）
  getAccountTableName(account) {
    const tableName = this.accountTableMap[account];
    if (!tableName) {
      throw new Error(`未知的账户: ${account}。支持的账户: ${Object.keys(this.accountTableMap).join(', ')}`);
    }
    return tableName;
  }

  // 验证账户表是否存在（新增方法）
  async validateAccountTable(account) {
    const tableName = this.getAccountTableName(account);
    
    try {
      // 尝试查询表是否存在（通过简单的 count 查询）
      const { error } = await this.supabase
        .from(tableName)
        .select('count')
        .limit(1);

      if (error && error.code === '42P01') { // 表不存在
        throw new Error(`账户表 ${tableName} 不存在`);
      }
      
      return true;
    } catch (error) {
      console.error(`❌ 账户表验证失败: ${error.message}`);
      return false;
    }
  }

  // 获取未清算的交易记录（增强版 - 支持按账户筛选）
  async getUnclearedDeals(limit = 50, account = null) {
    try {
      let query = this.supabase
        .from('dealLogs')
        .select('*')
        .eq('cleared', false)
        .order('dealID', { ascending: true });

      // 如果指定了账户，只获取该账户的交易
      if (account) {
        query = query.eq('account', account);
      }

      query = query.limit(limit);

      const { data, error } = await query;

      if (error) {
        console.error('❌ 获取未清算交易失败:', error.message);
        return null;
      }
      
      console.log(`✅ 获取到 ${data.length} 条未清算交易记录`);
      
      // 按账户分组统计
      const accountStats = {};
      data.forEach(deal => {
        accountStats[deal.account] = (accountStats[deal.account] || 0) + 1;
      });
      
      if (Object.keys(accountStats).length > 0) {
        console.log('   按账户分布:');
        Object.entries(accountStats).forEach(([acc, count]) => {
          console.log(`     ${acc}: ${count} 条`);
        });
      }
      
      return data;
      
    } catch (error) {
      console.error('❌ 获取未清算交易异常:', error.message);
      return null;
    }
  }

  // 核心业务流程：处理单条交易记录的清算（增强版）
  async handleClearDeal(dealRecord) {
    const startTime = Date.now();
    
    console.log(`\n🔄 开始处理交易记录 #${dealRecord.dealID}: ${dealRecord.account} - ${dealRecord.action} ${dealRecord.quantity} ${dealRecord.ticker}`);
    
    try {
      // 步骤1: 验证交易数据（增强版）
      if (!await this.validateDealRecord(dealRecord)) {
        await this.logger.logFailure(
          'clear_deal_validation',
          'dealLogs',
          dealRecord.dealID.toString(),
          '交易记录验证失败',
          { 
            account: dealRecord.account,
            ticker: dealRecord.ticker, 
            action: dealRecord.action 
          }
        );
        return false;
      }

      // 步骤2: 验证账户表是否存在
      if (!await this.validateAccountTable(dealRecord.account)) {
        await this.logger.logFailure(
          'clear_deal_account_validation',
          'dealLogs',
          dealRecord.dealID.toString(),
          '账户表不存在',
          { account: dealRecord.account }
        );
        return false;
      }

      // 步骤3: 处理账户表更新
      const accountUpdateSuccess = await this.processAccountTable(dealRecord);
      if (!accountUpdateSuccess) {
        await this.logger.logFailure(
          'clear_deal_account_update',
          this.getAccountTableName(dealRecord.account),
          dealRecord.ticker,
          '账户表更新失败',
          { 
            account: dealRecord.account,
            ticker: dealRecord.ticker, 
            action: dealRecord.action, 
            quantity: dealRecord.quantity 
          }
        );
        return false;
      }

      // 步骤4: 处理账本表更新
      const ledgerUpdateSuccess = await this.processLedgerTable(dealRecord);
      if (!ledgerUpdateSuccess) {
        await this.logger.logFailure(
          'clear_deal_ledger_update',
          'ledger',
          `${dealRecord.account}_CASH`,
          '账本表更新失败',
          { 
            account: dealRecord.account,
            cashChange: dealRecord.quantity * dealRecord.price, 
            action: dealRecord.action 
          }
        );
        return false;
      }

      // 步骤5: 标记交易为已清算
      const clearSuccess = await this.markDealAsCleared(dealRecord.dealID);
      if (!clearSuccess) {
        await this.logger.logFailure(
          'clear_deal_mark_cleared',
          'dealLogs',
          dealRecord.dealID.toString(),
          '标记清算状态失败',
          { account: dealRecord.account }
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
          account: dealRecord.account,
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
          account: dealRecord.account,
          ticker: dealRecord.ticker,
          action: dealRecord.action,
          duration: duration
        }
      );
      
      console.error(`❌ 处理交易记录 #${dealRecord.dealID} 失败:`, error.message);
      return false;
    }
  }

  // 验证交易记录（增强版）
  async validateDealRecord(dealRecord) {
    console.log('   📋 验证交易记录...');
    
    const requiredFields = ['dealID', 'account', 'action', 'ticker', 'quantity', 'price'];
    for (const field of requiredFields) {
      if (!dealRecord[field] && dealRecord[field] !== 0) {
        console.error(`❌ 交易记录缺少必要字段: ${field}`);
        return false;
      }
    }

    // 验证账户是否在映射表中
    if (!this.accountTableMap[dealRecord.account]) {
      console.error(`❌ 未知的账户: ${dealRecord.account}`);
      console.error(`   支持的账户: ${Object.keys(this.accountTableMap).join(', ')}`);
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

  // 处理账户表更新（增强版 - 支持多账户表）
  async processAccountTable(dealRecord) {
    const accountTable = this.getAccountTableName(dealRecord.account);
    console.log(`   📊 处理账户表更新: ${accountTable}`);
    
    try {
      // 检查账户表中是否已存在该股票记录
      const { data: existingHolding, error: queryError } = await this.supabase
        .from(accountTable)
        .select('*')
        .eq('ticker', dealRecord.ticker)
        .single();

      if (queryError && queryError.code !== 'PGRST116') {
        throw queryError;
      }

      if (existingHolding) {
        return await this.updateExistingHolding(accountTable, existingHolding, dealRecord);
      } else {
        return await this.createNewHolding(accountTable, dealRecord);
      }
      
    } catch (error) {
      console.error(`❌ 处理账户表失败:`, error.message);
      return false;
    }
  }

  // 更新现有持仓（保持不变）
  async updateExistingHolding(accountTable, existingHolding, dealRecord) {
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

    const updateData = {
      holding: newHolding,
      costPerShare: newCostPerShare,
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

  // 创建新持仓（增强版 - 设置正确的 accountID）
  async createNewHolding(accountTable, dealRecord) {
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
      accountID: dealRecord.account,  // 使用交易记录中的 account，而不是固定值
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

  // 处理账本表更新（保持不变）
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

  // 创建新的现金记录（保持不变）
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

  // 更新现金记录（保持不变）
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

  // 标记交易为已清算（保持不变）
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

  // 获取公司名称（保持不变）
  async getCompanyName(ticker) {
    const companyMap = {
      'AAPL': 'Apple Inc.', 'GOOGL': 'Alphabet Inc.', 'TSLA': 'Tesla Inc.',
      'MSFT': 'Microsoft Corporation', 'NVDA': 'NVIDIA Corporation',
      'APO': 'Apollo Global Management'
    };
    return companyMap[ticker] || `${ticker} Company`;
  }

  // 批量处理未清算交易（增强版 - 支持按账户处理）
  async processAllUnclearedDeals(account = null) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return false;
    }

    const accountInfo = account ? `(账户: ${account})` : '(所有账户)';
    console.log(`🚀 开始批量处理所有未清算交易 ${accountInfo}...`);
    
    const startTime = Date.now();
    let successCount = 0;
    let failCount = 0;

    try {
      const unclearedDeals = await this.getUnclearedDeals(100, account);
      
      if (!unclearedDeals || unclearedDeals.length === 0) {
        await this.logger.logSuccess(
          'batch_clear_deals',
          'dealLogs',
          'all',
          { 
            message: '没有未清算交易',
            account: account || 'all'
          }
        );
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
        data: { 
          account: account || 'all',
          total: unclearedDeals.length, 
          success: successCount, 
          failed: failCount,
          successRate: (successCount / unclearedDeals.length) * 100
        },
        status: failCount === 0 ? 'SUCCESS' : 'PARTIAL',
        duration: duration
      });

      console.log(`\n📊 批量处理完成: 成功 ${successCount} 条, 失败 ${failCount} 条, 耗时 ${duration}ms`);
      return failCount === 0;
      
    } catch (error) {
      const duration = Date.now() - startTime;
      
      await this.logger.logFailure(
        'batch_clear_deals_error',
        'dealLogs',
        'batch',
        error,
        {
          account: account || 'all',
          successCount,
          failCount,
          duration: duration
        }
      );
      
      console.error('❌ 批量处理失败:', error.message);
      return false;
    }
  }

  // 添加新账户映射（新增方法）
  addAccountMapping(accountCode, tableName) {
    this.accountTableMap[accountCode] = tableName;
    console.log(`✅ 添加账户映射: ${accountCode} -> ${tableName}`);
  }

  // 获取支持的账户列表（新增方法）
  getSupportedAccounts() {
    return Object.keys(this.accountTableMap);
  }

  // 显示账户统计（新增方法）
  async showAccountStats() {
    console.log('\n📊 账户统计信息');
    console.log('=' .repeat(40));
    
    console.log('支持的账户:');
    this.getSupportedAccounts().forEach(account => {
      console.log(`  ${account} -> ${this.accountTableMap[account]}`);
    });

    // 获取各账户的未清算交易数量
    const unclearedDeals = await this.getUnclearedDeals(1000); // 获取足够多的记录
    if (unclearedDeals) {
      const accountStats = {};
      unclearedDeals.forEach(deal => {
        accountStats[deal.account] = (accountStats[deal.account] || 0) + 1;
      });

      console.log('\n未清算交易分布:');
      if (Object.keys(accountStats).length === 0) {
        console.log('  暂无未清算交易');
      } else {
        Object.entries(accountStats).forEach(([account, count]) => {
          console.log(`  ${account}: ${count} 条`);
        });
      }
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
      
      // 显示账户统计
      await accounting.showAccountStats();
      
      // 处理所有账户的未清算交易
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
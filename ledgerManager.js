// ledger-manager.js
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

class LedgerManager {
  constructor() {
    this.supabase = createClient(
      process.env.SUPABASE_URL,
      process.env.SUPABASE_ANON_KEY
    );
    this.authenticated = false;
    this.tableName = 'ledger';
  }

  async initialize() {
    console.log('🔐 初始化账本管理器...');
    
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

  // 创建或更新账本记录
  async upsertLedgerRecord(recordData) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      // 确保有时间戳
      const dataWithTimestamp = {
        ...recordData,
        timeStamp: recordData.timeStamp || new Date().toISOString()
      };

      const { data, error } = await this.supabase
        .from(this.tableName)
        .upsert(dataWithTimestamp, { onConflict: 'assetID' })
        .select();

      if (error) throw error;
      
      console.log('✅ 账本记录保存成功, 资产ID:', recordData.assetID);
      return data[0];
      
    } catch (error) {
      console.error('❌ 保存账本记录失败:', error.message);
      return null;
    }
  }

  // 批量创建或更新账本记录
  async upsertMultipleLedgerRecords(recordsArray) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      // 为每条记录添加时间戳
      const recordsWithTimestamp = recordsArray.map(record => ({
        ...record,
        timeStamp: record.timeStamp || new Date().toISOString()
      }));

      const { data, error } = await this.supabase
        .from(this.tableName)
        .upsert(recordsWithTimestamp, { onConflict: 'assetID' })
        .select();

      if (error) throw error;
      
      console.log(`✅ 批量保存成功, 共处理 ${data.length} 条记录`);
      return data;
      
    } catch (error) {
      console.error('❌ 批量保存失败:', error.message);
      return null;
    }
  }

  // 获取所有账本记录
  async getAllLedgerRecords(sortBy = 'assetID') {
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
      
      console.log(`✅ 获取到 ${data.length} 条账本记录`);
      return data;
      
    } catch (error) {
      console.error('❌ 读取账本记录失败:', error.message);
      return null;
    }
  }

  // 根据资产ID查询特定记录
  async getLedgerRecordByAssetID(assetID) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from(this.tableName)
        .select('*')
        .eq('assetID', assetID)
        .single();

      if (error) throw error;
      
      console.log('✅ 查询成功:', assetID);
      return data;
      
    } catch (error) {
      console.error('❌ 查询账本记录失败:', error.message);
      return null;
    }
  }

  // 根据条件查询账本记录
  async getLedgerRecordsByCondition(conditions = {}) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      let query = this.supabase
        .from(this.tableName)
        .select('*');

      // 动态添加查询条件
      if (conditions.assetType) {
        query = query.eq('AssetType', conditions.assetType);
      }
      if (conditions.currency) {
        query = query.eq('Currency', conditions.currency);
      }
      if (conditions.minCash) {
        query = query.gte('Cash', conditions.minCash);
      }
      if (conditions.maxCash) {
        query = query.lte('Cash', conditions.maxCash);
      }
      if (conditions.minDebt) {
        query = query.gte('Debt', conditions.minDebt);
      }
      if (conditions.startDate) {
        query = query.gte('timeStamp', conditions.startDate);
      }
      if (conditions.endDate) {
        query = query.lte('timeStamp', conditions.endDate);
      }

      query = query.order('assetID', { ascending: true });

      const { data, error } = await query;

      if (error) throw error;
      
      console.log(`✅ 条件查询成功, 获取到 ${data.length} 条记录`);
      return data;
      
    } catch (error) {
      console.error('❌ 条件查询失败:', error.message);
      return null;
    }
  }

  // 更新现金余额
  async updateCashBalance(assetID, newCashBalance) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from(this.tableName)
        .update({ 
          Cash: newCashBalance,
          timeStamp: new Date().toISOString()
        })
        .eq('assetID', assetID)
        .select();

      if (error) throw error;
      
      if (data.length === 0) {
        console.log('⚠️ 未找到对应的账本记录');
        return null;
      }
      
      console.log('✅ 现金余额更新成功, 资产ID:', assetID, '新余额:', newCashBalance);
      return data[0];
      
    } catch (error) {
      console.error('❌ 更新现金余额失败:', error.message);
      return null;
    }
  }

  // 更新债务余额
  async updateDebtBalance(assetID, newDebtBalance) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from(this.tableName)
        .update({ 
          Debt: newDebtBalance,
          timeStamp: new Date().toISOString()
        })
        .eq('assetID', assetID)
        .select();

      if (error) throw error;
      
      console.log('✅ 债务余额更新成功, 资产ID:', assetID, '新债务:', newDebtBalance);
      return data[0];
      
    } catch (error) {
      console.error('❌ 更新债务余额失败:', error.message);
      return null;
    }
  }

  // 更新市值
  async updateMarketValue(assetID, marketValueCNY, valueTTMCNY = null) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const updateData = {
        marketValueCNY: marketValueCNY,
        timeStamp: new Date().toISOString()
      };

      if (valueTTMCNY !== null) {
        updateData.ValueTTMCNY = valueTTMCNY;
      }

      const { data, error } = await this.supabase
        .from(this.tableName)
        .update(updateData)
        .eq('assetID', assetID)
        .select();

      if (error) throw error;
      
      console.log('✅ 市值更新成功, 资产ID:', assetID, '新市值:', marketValueCNY);
      return data[0];
      
    } catch (error) {
      console.error('❌ 更新市值失败:', error.message);
      return null;
    }
  }

  // 现金交易（增加或减少现金）
  async processCashTransaction(assetID, amount, transactionType = 'deposit') {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      // 首先获取当前现金余额
      const currentRecord = await this.getLedgerRecordByAssetID(assetID);
      if (!currentRecord) {
        console.log('❌ 未找到资产记录');
        return null;
      }

      const currentCash = currentRecord.Cash || 0;
      let newCashBalance;

      if (transactionType === 'deposit') {
        newCashBalance = currentCash + amount;
      } else if (transactionType === 'withdraw') {
        newCashBalance = currentCash - amount;
      } else {
        throw new Error('交易类型必须是 deposit 或 withdraw');
      }

      // 更新现金余额
      return await this.updateCashBalance(assetID, newCashBalance);
      
    } catch (error) {
      console.error('❌ 处理现金交易失败:', error.message);
      return null;
    }
  }

  // 债务交易（增加或减少债务）
  async processDebtTransaction(assetID, amount, transactionType = 'borrow') {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      // 首先获取当前债务余额
      const currentRecord = await this.getLedgerRecordByAssetID(assetID);
      if (!currentRecord) {
        console.log('❌ 未找到资产记录');
        return null;
      }

      const currentDebt = currentRecord.Debt || 0;
      let newDebtBalance;

      if (transactionType === 'borrow') {
        newDebtBalance = currentDebt + amount;
      } else if (transactionType === 'repay') {
        newDebtBalance = currentDebt - amount;
      } else {
        throw new Error('交易类型必须是 borrow 或 repay');
      }

      // 更新债务余额
      return await this.updateDebtBalance(assetID, newDebtBalance);
      
    } catch (error) {
      console.error('❌ 处理债务交易失败:', error.message);
      return null;
    }
  }

  // 删除账本记录
  async deleteLedgerRecord(assetID) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { error } = await this.supabase
        .from(this.tableName)
        .delete()
        .eq('assetID', assetID);

      if (error) throw error;
      
      console.log('✅ 账本记录删除成功, 资产ID:', assetID);
      return true;
      
    } catch (error) {
      console.error('❌ 删除账本记录失败:', error.message);
      return false;
    }
  }

  // 获取账本统计信息
  async getLedgerStats() {
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
        totalRecords: data.length,
        totalCash: data.reduce((sum, record) => sum + (record.Cash || 0), 0),
        totalDebt: data.reduce((sum, record) => sum + (record.Debt || 0), 0),
        totalMarketValue: data.reduce((sum, record) => sum + (record.marketValueCNY || 0), 0),
        totalTTMValue: data.reduce((sum, record) => sum + (record.ValueTTMCNY || 0), 0),
        netAssetValue: 0,
        byAssetType: data.reduce((acc, record) => {
          acc[record.AssetType] = (acc[record.AssetType] || 0) + 1;
          return acc;
        }, {}),
        byCurrency: data.reduce((acc, record) => {
          acc[record.Currency] = (acc[record.Currency] || 0) + 1;
          return acc;
        }, {})
      };

      stats.netAssetValue = stats.totalCash - stats.totalDebt + stats.totalMarketValue;

      console.log('📊 账本统计信息:');
      console.log(`   总记录数: ${stats.totalRecords}`);
      console.log(`   总现金: ${stats.totalCash.toFixed(2)}`);
      console.log(`   总债务: ${stats.totalDebt.toFixed(2)}`);
      console.log(`   总市值: ${stats.totalMarketValue.toFixed(2)}`);
      console.log(`   净资产: ${stats.netAssetValue.toFixed(2)}`);
      
      return stats;
      
    } catch (error) {
      console.error('❌ 获取统计信息失败:', error.message);
      return null;
    }
  }

  // 获取资产负债表快照
  async getBalanceSheetSnapshot() {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from(this.tableName)
        .select('*');

      if (error) throw error;

      const balanceSheet = {
        timestamp: new Date().toISOString(),
        assets: {
          cash: data.reduce((sum, record) => sum + (record.Cash || 0), 0),
          investments: data.reduce((sum, record) => sum + (record.marketValueCNY || 0), 0),
          totalAssets: 0
        },
        liabilities: {
          debt: data.reduce((sum, record) => sum + (record.Debt || 0), 0),
          totalLiabilities: 0
        },
        equity: 0
      };

      balanceSheet.assets.totalAssets = balanceSheet.assets.cash + balanceSheet.assets.investments;
      balanceSheet.liabilities.totalLiabilities = balanceSheet.liabilities.debt;
      balanceSheet.equity = balanceSheet.assets.totalAssets - balanceSheet.liabilities.totalLiabilities;

      console.log('💰 资产负债表快照:');
      console.log('   资产:');
      console.log(`     现金: ${balanceSheet.assets.cash.toFixed(2)}`);
      console.log(`     投资: ${balanceSheet.assets.investments.toFixed(2)}`);
      console.log(`     总资产: ${balanceSheet.assets.totalAssets.toFixed(2)}`);
      console.log('   负债:');
      console.log(`     债务: ${balanceSheet.liabilities.debt.toFixed(2)}`);
      console.log(`     总负债: ${balanceSheet.liabilities.totalLiabilities.toFixed(2)}`);
      console.log('   净资产:');
      console.log(`     所有者权益: ${balanceSheet.equity.toFixed(2)}`);

      return balanceSheet;
      
    } catch (error) {
      console.error('❌ 获取资产负债表失败:', error.message);
      return null;
    }
  }

  // 获取最近更新的记录
  async getRecentRecords(limit = 10) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from(this.tableName)
        .select('*')
        .order('timeStamp', { ascending: false })
        .limit(limit);

      if (error) throw error;
      
      console.log(`✅ 获取到最近 ${data.length} 条更新记录`);
      return data;
      
    } catch (error) {
      console.error('❌ 获取最近记录失败:', error.message);
      return null;
    }
  }
}

// 使用示例
async function runDemo() {
  const manager = new LedgerManager();
  
  // 1. 初始化
  const initialized = await manager.initialize();
  if (!initialized) return;

  console.log('\n' + '='.repeat(50));
  console.log('🚀 开始演示账本管理操作');
  console.log('='.repeat(50) + '\n');

  // 2. 创建或更新账本记录
  console.log('1. 创建/更新账本记录...');
  const newRecord = await manager.upsertLedgerRecord({
    assetID: 'CASH_USD',
    AssetType: 'cash',
    Currency: 'USD',
    Cash: 50000.00,
    Debt: 0,
    marketValueCNY: 0,
    ValueTTMCNY: 0
  });

  // 3. 批量创建记录
  console.log('\n2. 批量创建账本记录...');
  const batchRecords = await manager.upsertMultipleLedgerRecords([
    {
      assetID: 'STOCK_PORTFOLIO',
      AssetType: 'equity',
      Currency: 'USD',
      Cash: 0,
      Debt: 0,
      marketValueCNY: 350000.00,
      ValueTTMCNY: 320000.00
    },
    {
      assetID: 'MARGIN_LOAN',
      AssetType: 'debt',
      Currency: 'USD',
      Cash: 0,
      Debt: 25000.00,
      marketValueCNY: 0,
      ValueTTMCNY: 0
    },
    {
      assetID: 'REAL_ESTATE',
      AssetType: 'property',
      Currency: 'CNY',
      Cash: 0,
      Debt: 500000.00,
      marketValueCNY: 1200000.00,
      ValueTTMCNY: 1150000.00
    }
  ]);

  // 4. 读取所有记录
  console.log('\n3. 读取所有账本记录...');
  const allRecords = await manager.getAllLedgerRecords();
  if (allRecords) {
    allRecords.forEach(record => {
      console.log(`   ${record.assetID}: 现金 $${record.Cash}, 债务 $${record.Debt}, 市值 ¥${record.marketValueCNY}`);
    });
  }

  // 5. 现金交易
  console.log('\n4. 处理现金存款...');
  await manager.processCashTransaction('CASH_USD', 10000.00, 'deposit');

  // 6. 债务交易
  console.log('\n5. 处理借款...');
  await manager.processDebtTransaction('MARGIN_LOAN', 5000.00, 'borrow');

  // 7. 更新市值
  console.log('\n6. 更新投资组合市值...');
  await manager.updateMarketValue('STOCK_PORTFOLIO', 380000.00, 350000.00);

  // 8. 获取统计信息
  console.log('\n7. 获取账本统计信息...');
  await manager.getLedgerStats();

  // 9. 资产负债表
  console.log('\n8. 生成资产负债表...');
  await manager.getBalanceSheetSnapshot();

  // 10. 最近记录
  console.log('\n9. 获取最近更新记录...');
  const recentRecords = await manager.getRecentRecords(5);

  console.log('\n' + '='.repeat(50));
  console.log('🎉 账本管理演示完成!');
  console.log('='.repeat(50));
}

// 运行演示
if (require.main === module) {
  runDemo().catch(console.error);
}

// 导出类供其他模块使用
module.exports = LedgerManager;
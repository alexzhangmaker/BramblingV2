// deal-logs-manager.js
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

class DealLogsManager {
  constructor() {
    this.supabase = createClient(
      process.env.SUPABASE_URL,
      process.env.SUPABASE_ANON_KEY
    );
    this.authenticated = false;
  }

  async initialize() {
    console.log('🔐 初始化 DealLogs 管理器...');
    
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

  // 创建交易记录
  async createDealLog(dealData) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from('dealLogs')
        .insert(dealData)
        .select();

      if (error) throw error;
      
      console.log('✅ 交易记录创建成功, ID:', data[0].dealID);
      return data[0];
      
    } catch (error) {
      console.error('❌ 创建交易记录失败:', error.message);
      return null;
    }
  }

  // 批量创建交易记录
  async createMultipleDealLogs(dealLogsArray) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from('dealLogs')
        .insert(dealLogsArray)
        .select();

      if (error) throw error;
      
      console.log(`✅ 批量创建成功, 共 ${data.length} 条记录`);
      return data;
      
    } catch (error) {
      console.error('❌ 批量创建失败:', error.message);
      return null;
    }
  }

  // 读取所有交易记录
  async getAllDealLogs(limit = 50) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from('dealLogs')
        .select('*')
        .order('dealID', { ascending: false })
        .limit(limit);

      if (error) throw error;
      
      console.log(`✅ 获取到 ${data.length} 条交易记录`);
      return data;
      
    } catch (error) {
      console.error('❌ 读取交易记录失败:', error.message);
      return null;
    }
  }

  // 根据条件查询交易记录
  async getDealLogsByCondition(conditions = {}) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      let query = this.supabase
        .from('dealLogs')
        .select('*');

      // 动态添加查询条件
      if (conditions.account) {
        query = query.eq('account', conditions.account);
      }
      if (conditions.ticker) {
        query = query.eq('ticker', conditions.ticker);
      }
      if (conditions.action) {
        query = query.eq('action', conditions.action);
      }
      if (conditions.cleared !== undefined) {
        query = query.eq('cleared', conditions.cleared);
      }
      if (conditions.startDate && conditions.endDate) {
        query = query.gte('date', conditions.startDate).lte('date', conditions.endDate);
      }

      query = query.order('dealID', { ascending: false });

      const { data, error } = await query;

      if (error) throw error;
      
      console.log(`✅ 条件查询成功, 获取到 ${data.length} 条记录`);
      return data;
      
    } catch (error) {
      console.error('❌ 条件查询失败:', error.message);
      return null;
    }
  }

  // 更新交易记录
  async updateDealLog(dealID, updates) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from('dealLogs')
        .update(updates)
        .eq('dealID', dealID)
        .select();

      if (error) throw error;
      
      if (data.length === 0) {
        console.log('⚠️ 未找到对应的交易记录');
        return null;
      }
      
      console.log('✅ 交易记录更新成功, ID:', data[0].dealID);
      return data[0];
      
    } catch (error) {
      console.error('❌ 更新交易记录失败:', error.message);
      return null;
    }
  }

  // 标记交易为已清算
  async markAsCleared(dealID) {
    return await this.updateDealLog(dealID, { cleared: true });
  }

  // 删除交易记录
  async deleteDealLog(dealID) {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { error } = await this.supabase
        .from('dealLogs')
        .delete()
        .eq('dealID', dealID);

      if (error) throw error;
      
      console.log('✅ 交易记录删除成功, ID:', dealID);
      return true;
      
    } catch (error) {
      console.error('❌ 删除交易记录失败:', error.message);
      return false;
    }
  }

  // 获取统计信息
  async getStats() {
    if (!this.authenticated) {
      console.log('⚠️ 请先调用 initialize() 方法初始化');
      return null;
    }

    try {
      const { data, error } = await this.supabase
        .from('dealLogs')
        .select('*');

      if (error) throw error;

      const stats = {
        total: data.length,
        buyCount: data.filter(d => d.action === 'BUY').length,
        sellCount: data.filter(d => d.action === 'SELL').length,
        clearedCount: data.filter(d => d.cleared).length,
        uniqueTickers: [...new Set(data.map(d => d.ticker))],
        uniqueAccounts: [...new Set(data.filter(d => d.account).map(d => d.account))]
      };

      console.log('📊 统计信息:', stats);
      return stats;
      
    } catch (error) {
      console.error('❌ 获取统计信息失败:', error.message);
      return null;
    }
  }
}

// 使用示例
async function runDemo() {
  const manager = new DealLogsManager();
  
  // 1. 初始化
  const initialized = await manager.initialize();
  if (!initialized) return;

  console.log('\n' + '='.repeat(50));
  console.log('🚀 开始演示 dealLogs 表操作');
  console.log('='.repeat(50) + '\n');

  // 2. 创建单条记录
  console.log('1. 创建单条交易记录...');
  const newDeal = await manager.createDealLog({
    account: 'ALEX001',
    action: 'BUY',
    ticker: 'AAPL',
    price: 150.50,
    quantity: 100,
    market: 'NASDAQ',
    date: '2024-01-15',
    cleared: false
  });

  // 3. 批量创建记录
  console.log('\n2. 批量创建交易记录...');
  const batchDeals = await manager.createMultipleDealLogs([
    {
      account: 'ALEX001',
      action: 'SELL',
      ticker: 'GOOGL',
      price: 2800.75,
      quantity: 10,
      market: 'NASDAQ',
      date: '2024-01-16',
      cleared: true
    },
    {
      account: 'ALEX002',
      action: 'BUY',
      ticker: 'TSLA',
      price: 250.30,
      quantity: 50,
      market: 'NASDAQ',
      date: '2024-01-17',
      cleared: false
    },
    {
      account: 'ALEX001',
      action: 'BUY',
      ticker: 'MSFT',
      price: 380.20,
      quantity: 25,
      market: 'NASDAQ',
      date: '2024-01-18',
      cleared: false
    }
  ]);

  // 4. 读取所有记录
  console.log('\n3. 读取所有交易记录...');
  const allDeals = await manager.getAllDealLogs(10);
  if (allDeals) {
    allDeals.forEach(deal => {
      console.log(`   ID: ${deal.dealID}, ${deal.action} ${deal.quantity} ${deal.ticker} @ $${deal.price}`);
    });
  }

  // 5. 条件查询
  console.log('\n4. 条件查询 - ALEX001 的未清算交易...');
  const alexDeals = await manager.getDealLogsByCondition({
    account: 'ALEX001',
    cleared: false
  });

  // 6. 更新记录
  if (newDeal) {
    console.log('\n5. 更新交易记录...');
    await manager.updateDealLog(newDeal.dealID, {
      price: 151.25,
      quantity: 120
    });
  }

  // 7. 标记为已清算
  if (newDeal) {
    console.log('\n6. 标记交易为已清算...');
    await manager.markAsCleared(newDeal.dealID);
  }

  // 8. 获取统计信息
  console.log('\n7. 获取统计信息...');
  await manager.getStats();

  // 9. 删除记录 (可选，注释掉以避免删除数据)
  /*
  console.log('\n8. 删除交易记录...');
  if (newDeal) {
    await manager.deleteDealLog(newDeal.dealID);
  }
  */

  console.log('\n' + '='.repeat(50));
  console.log('🎉 演示完成!');
  console.log('='.repeat(50));
}

// 运行演示
runDemo().catch(console.error);

// 导出类供其他模块使用
module.exports = DealLogsManager;
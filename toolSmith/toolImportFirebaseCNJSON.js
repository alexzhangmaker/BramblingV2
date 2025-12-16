// importHoldingsToFirebase.js
const admin = require('firebase-admin');


// 初始化Firebase
const serviceAccount = require('/Users/zhangqing/Documents/Github/serviceKeys/bramblingV2Firebase.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: 'https://outpost-8d74e-14018.firebaseio.com/'
});

const db = admin.database();

// 持仓数据
const holdingsData = [
  {
    "ticker": "0966.HK",
    "company": "中国太平",
    "description": "中国太平",
    "holding": 2000,
    "costPerShare": 17.97,
    "currency": "HKD",
    "exchange": "HK",
    "exchangeCode": "HK",
    "assetClass": "STK"
  },
  {
    "ticker": "1088.HK",
    "company": "中国神华",
    "description": "中国神华",
    "holding": 2000,
    "costPerShare": 14.75,
    "currency": "HKD",
    "exchange": "HK",
    "exchangeCode": "HK",
    "assetClass": "STK"
  },
  {
    "ticker": "1171.HK",
    "company": "衮矿能源",
    "description": "衮矿能源",
    "holding": 3900,
    "costPerShare": 3.26,
    "currency": "HKD",
    "exchange": "HK",
    "exchangeCode": "HK",
    "assetClass": "STK"
  }
];

/**
 * 清理ticker字符串，使其符合Firebase路径要求
 * 将 "." 转换为 "_"，" " 转换为 "__"
 */
function sanitizeTicker(ticker) {
  if (!ticker) return 'UNKNOWN';
  
  return ticker
    .replace(/\./g, '_')        // 将 . 替换为 _
    .replace(/\s+/g, '__')      // 将空格替换为 __
    .replace(/[#\$\[\]\/]/g, '_') // 替换其他可能不合法的字符
    .toUpperCase();
}

/**
 * 将持仓数据导入到指定账户
 */
async function importHoldingsToAccount(accountId, holdings) {
  try {
    console.log(`📁 开始导入持仓数据到账户: ${accountId}`);
    console.log(`📊 准备导入 ${holdings.length} 个持仓记录`);

    // 构建holdings对象
    const holdingsObject = {};
    let totalHoldings = 0;
    
    for (const holding of holdings) {
      const sanitizedTicker = sanitizeTicker(holding.ticker);
      holdingsObject[sanitizedTicker] = {
        ticker: holding.ticker,
        company: holding.company,
        description: holding.description,
        holding: holding.holding,
        costPerShare: holding.costPerShare,
        currency: holding.currency,
        exchange: holding.exchange,
        exchangeCode: holding.exchangeCode,
        assetClass: holding.assetClass
      };
      
      totalHoldings += holding.holding;
      console.log(`✅ 处理持仓: ${holding.ticker} -> ${sanitizedTicker}`);
    }

    // 获取账户现有数据（如果有）
    let accountData = {};
    try {
      const snapshot = await db.ref(`accounts/${accountId}`).once('value');
      accountData = snapshot.val() || {};
    } catch (error) {
      console.log('ℹ️ 账户不存在或无法读取，将创建新账户');
    }

    // 更新账户数据
    const updatedAccountData = {
      ...accountData,
      holdings: holdingsObject,
      meta: {
        ...(accountData.meta || {}),
        Country: "CN",
        currency: "CNY",
        lastUpdated: new Date().toISOString(),
        source: "Manual Import",
        totalHoldings: totalHoldings
      }
    };

    // 如果没有现金数据，添加默认现金数据
    if (!updatedAccountData.cash) {
      updatedAccountData.cash = { CNY: 0 };
    }
    
    // 如果没有债务数据，添加默认债务数据
    if (!updatedAccountData.debt) {
      updatedAccountData.debt = { CNY: 0 };
    }

    // 写入Firebase
    await db.ref(`accounts/${accountId}`).set(updatedAccountData);
    
    console.log(`\n✅ 成功导入 ${holdings.length} 个持仓到账户 ${accountId}`);
    console.log(`💰 总持仓数量: ${totalHoldings}`);
    console.log(`📈 持仓标的数量: ${Object.keys(holdingsObject).length}`);
    
    // 显示汇总信息
    const currencySummary = {};
    holdings.forEach(holding => {
      const currency = holding.currency;
      if (!currencySummary[currency]) {
        currencySummary[currency] = 0;
      }
      currencySummary[currency] += holding.holding * holding.costPerShare;
    });
    
    console.log('\n💱 货币分布:');
    Object.entries(currencySummary).forEach(([currency, amount]) => {
      console.log(`  ${currency}: ${amount.toFixed(2)}`);
    });
    
    const assetClassSummary = {};
    holdings.forEach(holding => {
      const assetClass = holding.assetClass;
      if (!assetClassSummary[assetClass]) {
        assetClassSummary[assetClass] = 0;
      }
      assetClassSummary[assetClass] += holding.holding * holding.costPerShare;
    });
    
    console.log('\n📊 资产类别分布:');
    Object.entries(assetClassSummary).forEach(([assetClass, amount]) => {
      console.log(`  ${assetClass}: ${amount.toFixed(2)}`);
    });

    return {
      accountId,
      holdingsCount: holdings.length,
      totalHoldings,
      currencySummary,
      assetClassSummary
    };

  } catch (error) {
    console.error(`❌ 导入持仓数据失败:`, error.message);
    throw error;
  }
}

/**
 * 显示账户信息
 */
async function displayAccountInfo(accountId) {
  try {
    console.log(`\n🔍 检查账户 ${accountId} 信息...`);
    
    const snapshot = await db.ref(`accounts/${accountId}`).once('value');
    const accountData = snapshot.val();
    
    if (!accountData) {
      console.log('❌ 账户不存在');
      return;
    }
    
    console.log('💰 现金:', accountData.cash);
    console.log('📊 持仓数量:', Object.keys(accountData.holdings || {}).length);
    
    if (accountData.holdings) {
      console.log('\n📈 持仓详情:');
      Object.entries(accountData.holdings).forEach(([key, holding]) => {
        console.log(`  ${key}: ${holding.holding}股 @ ${holding.costPerShare} ${holding.currency} - ${holding.company}`);
      });
    }
    
  } catch (error) {
    console.error('❌ 获取账户数据失败:', error.message);
  }
}

/**
 * 主函数
 */
async function main() {
  const args = process.argv.slice(2);
  
  if (args.length === 0) {
    console.log(`
📋 使用方法:
  node importHoldingsToFirebase.js <账户ID>

示例:
  node importHoldingsToFirebase.js HTZQ
  node importHoldingsToFirebase.js NEW_ACCOUNT

说明:
  此脚本将把预定义的持仓数据导入到指定的Firebase账户中
  如果账户不存在，将自动创建新账户
    `);
    return;
  }

  const accountId = args[0];
  
  try {
    console.log('🚀 开始导入持仓数据到Firebase...');
    
    // 导入持仓数据
    const result = await importHoldingsToAccount(accountId, holdingsData);
    
    // 显示导入后的账户信息
    await displayAccountInfo(accountId);
    
    console.log('\n🎉 导入完成！');
    
  } catch (error) {
    console.error('❌ 导入过程失败:', error.message);
    process.exit(1);
  } finally {
    // 关闭Firebase连接
    admin.app().delete();
  }
}

// 运行主函数
main().catch(console.error);

module.exports = {
  importHoldingsToAccount,
  sanitizeTicker,
  holdingsData
};
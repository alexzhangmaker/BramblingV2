// csvToFirebase.js
const admin = require('firebase-admin');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');

// 初始化Firebase
const serviceAccount = require('/Users/zhangqing/Documents/Github/serviceKeys/bramblingV2Firebase.json');
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: 'https://outpost-8d74e-14018.firebaseio.com/'
});

const db = admin.database();


/**
 * 交易所代码映射表
 */
const EXCHANGE_MAP = {
  'NYSE': 'New York Stock Exchange',
  'NASDAQ': 'NASDAQ',
  'ARCA': 'NYSE Arca',
  'AMEX': 'NYSE American',
  'BATS': 'CBOE BZX',
  'IEX': 'Investors Exchange',
  'SS': 'Shanghai Stock Exchange',
  'SZ': 'Shenzhen Stock Exchange',
  'HK': 'Hong Kong Stock Exchange',
  'T': 'Tokyo Stock Exchange',
  'L': 'London Stock Exchange',
  'F': 'Frankfurt Stock Exchange',
  'PA': 'Paris Stock Exchange',
  'BR': 'Brussels Stock Exchange',
  'AS': 'Amsterdam Stock Exchange',
  'MI': 'Milan Stock Exchange',
  'SW': 'Swiss Exchange',
  'V': 'TSX Venture Exchange',
  'TO': 'Toronto Stock Exchange',
  'AX': 'Australian Securities Exchange',
  'SI': 'Singapore Exchange',
  'BK': 'Stock Exchange of Thailand',
  'KS': 'Korea Exchange',
  'JK': 'Indonesia Stock Exchange',
  'NS': 'National Stock Exchange of India',
  'BO': 'Bombay Stock Exchange'
};

/**
 * 获取交易所全名
 */
function getExchangeFullName(exchangeCode) {
  if (!exchangeCode) return 'Unknown Exchange';
  return EXCHANGE_MAP[exchangeCode] || exchangeCode;
}

/**
 * 判断是否为美国国债
 */
function isUSTreasury(description, assetClass) {
  if (!description) return false;
  
  const desc = description.toLowerCase();
  const asset = (assetClass || '').toLowerCase();
  
  return asset === 'bond' || 
         desc.includes('treasury') || 
         desc.includes('t-bill') ||
         desc.includes('t bill') ||
         desc.includes('government bond') ||
         desc.includes('govt bond');
}

/**
 * 清理ticker字符串，使其符合Firebase路径要求
 */
function sanitizeTicker(ticker, description = '', assetClass = '') {
  if (!ticker) return 'UNKNOWN';
  
  // 如果是美国国债，统一使用 US_TBill
  if (isUSTreasury(description, assetClass)) {
    return 'US_TBill';
  }
  
  return ticker
    .replace(/\./g, '_')        // 将 . 替换为 _
    .replace(/\s+/g, '__')      // 将空格替换为 __
    .replace(/[#\$\[\]\/]/g, '_') // 替换其他可能不合法的字符
    .toUpperCase();
}

/**
 * 从CSV文件中提取账户ID
 */
function extractAccountIdFromFilename(filename) {
  // 假设文件名格式为: IB7075_20251008.csv
  const match = filename.match(/^([A-Z0-9]+)_\d+\.csv$/i);
  return match ? match[1] : null;
}

/**
 * 从ticker中推断交易所（如果没有提供ListingExchange）
 */
function inferExchangeFromTicker(ticker) {
  if (!ticker) return 'Unknown';
  
  // 检查ticker后缀
  if (ticker.endsWith('.SS')) return 'SS';
  if (ticker.endsWith('.SZ')) return 'SZ';
  if (ticker.endsWith('.HK')) return 'HK';
  if (ticker.endsWith('.T')) return 'T';
  if (ticker.endsWith('.L')) return 'L';
  if (ticker.endsWith('.F')) return 'F';
  if (ticker.endsWith('.PA')) return 'PA';
  if (ticker.endsWith('.BR')) return 'BR';
  if (ticker.endsWith('.AS')) return 'AS';
  if (ticker.endsWith('.MI')) return 'MI';
  if (ticker.endsWith('.SW')) return 'SW';
  if (ticker.endsWith('.V')) return 'V';
  if (ticker.endsWith('.TO')) return 'TO';
  if (ticker.endsWith('.AX')) return 'AX';
  if (ticker.endsWith('.SI')) return 'SI';
  if (ticker.endsWith('.BK')) return 'BK';
  if (ticker.endsWith('.KS')) return 'KS';
  if (ticker.endsWith('.JK')) return 'JK';
  if (ticker.endsWith('.NS')) return 'NS';
  if (ticker.endsWith('.BO')) return 'BO';
  
  return 'Unknown';
}

/**
 * 将CSV行数据转换为Firebase持仓格式
 */
function convertCsvRowToHolding(row) {
  const ticker = row.Symbol || row.SYMBOL || '';
  const description = row.Description || row.DESCRIPTION || '';
  const assetClass = row.AssetClass || row.ASSETCLASS || '';
  
  const sanitizedTicker = sanitizeTicker(ticker, description, assetClass);
  
  // 获取交易所信息
  let exchangeCode = row.ListingExchange || row.LISTINGEXCHANGE || '';
  if (!exchangeCode) {
    exchangeCode = inferExchangeFromTicker(ticker);
  }
  const exchange = getExchangeFullName(exchangeCode);
  
  return {
    company: description,
    costPerShare: parseFloat(row.CostBasisPrice || row.COSTBASISPRICE || 0),
    currency: row.CurrencyPrimary || row.CURRENCYPRIMARY || 'USD',
    holding: parseInt(row.Quantity || row.QUANTITY || 0),
    ticker: ticker,
    exchange: exchange,
    exchangeCode: exchangeCode,
    description: description,
    assetClass: assetClass
  };
}

/**
 * 合并美国国债持仓
 */
function mergeUSTreasuryHoldings(holdings) {
  const usTreasuryKey = 'US_TBill';
  let mergedTreasury = null;
  const otherHoldings = {};
  
  Object.entries(holdings).forEach(([key, holding]) => {
    if (key === usTreasuryKey || isUSTreasury(holding.description, holding.assetClass)) {
      if (!mergedTreasury) {
        mergedTreasury = {
          company: 'US Treasury Bills',
          costPerShare: 0,
          currency: holding.currency,
          holding: 0,
          ticker: 'US_TBill',
          exchange: 'US Treasury',
          exchangeCode: 'UST',
          description: 'US Treasury Bills Aggregate',
          assetClass: 'BOND',
          components: []
        };
      }
      
      const totalCost = mergedTreasury.holding * mergedTreasury.costPerShare + 
                       holding.holding * holding.costPerShare;
      
      mergedTreasury.holding += holding.holding;
      
      if (mergedTreasury.holding > 0) {
        mergedTreasury.costPerShare = totalCost / mergedTreasury.holding;
      }
      
      mergedTreasury.components.push({
        originalTicker: holding.ticker,
        description: holding.description,
        holding: holding.holding,
        costPerShare: holding.costPerShare,
        exchange: holding.exchange
      });
    } else {
      otherHoldings[key] = holding;
    }
  });
  
  if (mergedTreasury) {
    otherHoldings[usTreasuryKey] = mergedTreasury;
    console.log(`💰 合并美国国债: ${mergedTreasury.holding}股 @ ${mergedTreasury.costPerShare.toFixed(4)} ${mergedTreasury.currency}`);
  }
  
  return otherHoldings;
}

/**
 * 处理CSV文件并上传到Firebase
 */
async function processCsvToFirebase(csvFilePath, accountId = null) {
  return new Promise((resolve, reject) => {
    if (!accountId) {
      const filename = path.basename(csvFilePath);
      accountId = extractAccountIdFromFilename(filename);
    }

    if (!accountId) {
      return reject(new Error('无法从文件名中提取账户ID，请手动指定'));
    }

    console.log(`📁 处理文件: ${csvFilePath}`);
    console.log(`👤 账户ID: ${accountId}`);

    const holdings = {};
    let cashData = {};

    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on('data', (row) => {
        try {
          const description = (row.Description || row.DESCRIPTION || '').toLowerCase();
          const assetClass = (row.AssetClass || row.ASSETCLASS || '').toLowerCase();
          const symbol = row.Symbol || row.SYMBOL || '';
          
          // 处理现金类资产
          if (assetClass === 'fund' || description.includes('cash') || 
              description.includes('treasury') || symbol === 'SGOV') {
            const currency = row.CurrencyPrimary || row.CURRENCYPRIMARY || 'USD';
            const positionValue = parseFloat(row.PositionValue || row.POSITIONVALUE || 0);
            
            if (!cashData[currency]) {
              cashData[currency] = 0;
            }
            cashData[currency] += positionValue;
            console.log(`💰 现金类资产: ${symbol} - ${positionValue} ${currency}`);
            return;
          }

          // 处理持仓数据
          const holding = convertCsvRowToHolding(row);
          const sanitizedTicker = sanitizeTicker(holding.ticker, holding.description, holding.assetClass);
          
          if (sanitizedTicker && holding.holding > 0) {
            // 如果已经存在相同ticker的持仓，合并它们
            if (holdings[sanitizedTicker]) {
              const existing = holdings[sanitizedTicker];
              const totalCost = existing.holding * existing.costPerShare + 
                              holding.holding * holding.costPerShare;
              
              existing.holding += holding.holding;
              if (existing.holding > 0) {
                existing.costPerShare = totalCost / existing.holding;
              }
              console.log(`🔄 合并持仓: ${sanitizedTicker} -> ${existing.holding}股`);
            } else {
              holdings[sanitizedTicker] = holding;
              console.log(`📊 处理持仓: ${holding.ticker} -> ${sanitizedTicker} [${holding.exchangeCode}]`);
            }
          }
        } catch (error) {
          console.error(`❌ 处理行数据失败:`, error.message);
        }
      })
      .on('end', async () => {
        try {
          console.log(`\n✅ CSV文件读取完成`);
          console.log(`📈 找到 ${Object.keys(holdings).length} 个持仓`);
          console.log(`💰 现金数据:`, cashData);

          // 合并美国国债持仓
          const mergedHoldings = mergeUSTreasuryHoldings(holdings);
          console.log(`📊 合并后持仓数量: ${Object.keys(mergedHoldings).length}`);

          // 构建账户数据结构
          const accountData = {
            cash: cashData,
            debt: { CNY: 0 },
            holdings: mergedHoldings,
            meta: {
              Country: "US",
              currency: "USD",
              lastUpdated: new Date().toISOString(),
              source: "CSV Import"
            }
          };

          // 上传到Firebase
          console.log(`\n🚀 上传数据到Firebase...`);
          const accountRef = db.ref(`accounts/${accountId}`);
          await accountRef.set(accountData);
          
          console.log(`✅ 成功上传账户 ${accountId} 数据到Firebase`);
          console.log(`📊 持仓数量: ${Object.keys(mergedHoldings).length}`);
          console.log(`💰 现金总额:`, cashData);
          
          // 显示持仓摘要
          console.log('\n📋 持仓摘要:');
          Object.entries(mergedHoldings).forEach(([key, holding]) => {
            console.log(`  ${key}: ${holding.holding}股 @ ${holding.costPerShare.toFixed(2)} ${holding.currency} [${holding.exchangeCode}]`);
          });
          
          resolve({
            accountId,
            holdingsCount: Object.keys(mergedHoldings).length,
            cashData,
            holdings: mergedHoldings
          });
        } catch (error) {
          reject(error);
        }
      })
      .on('error', (error) => {
        reject(error);
      });
  });
}

/**
 * 批量处理目录下的所有CSV文件
 */
async function processCsvDirectory(directoryPath) {
  try {
    console.log(`📂 扫描目录: ${directoryPath}`);
    
    const files = fs.readdirSync(directoryPath);
    const csvFiles = files.filter(file => file.endsWith('.csv'));
    
    console.log(`📁 找到 ${csvFiles.length} 个CSV文件`);
    
    const results = [];
    
    for (const csvFile of csvFiles) {
      try {
        const csvFilePath = path.join(directoryPath, csvFile);
        const result = await processCsvToFirebase(csvFilePath);
        results.push(result);
        console.log('---');
      } catch (error) {
        console.error(`❌ 处理文件 ${csvFile} 失败:`, error.message);
        results.push({ file: csvFile, error: error.message });
      }
    }
    
    return results;
  } catch (error) {
    console.error('❌ 处理目录失败:', error.message);
    throw error;
  }
}

/**
 * 显示账户数据
 */
async function displayAccountData(accountId) {
  try {
    console.log(`\n🔍 显示账户 ${accountId} 数据...`);
    
    const snapshot = await db.ref(`accounts/${accountId}`).once('value');
    const accountData = snapshot.val();
    
    if (!accountData) {
      console.log('❌ 账户不存在');
      return;
    }
    
    console.log('💰 现金:', accountData.cash);
    console.log('📊 持仓数量:', Object.keys(accountData.holdings || {}).length);
    
    console.log('\n📈 持仓详情:');
    Object.entries(accountData.holdings || {}).forEach(([key, holding]) => {
      console.log(`  ${key}: ${holding.holding}股 @ ${holding.costPerShare} ${holding.currency}`);
      console.log(`    交易所: ${holding.exchange} [${holding.exchangeCode}]`);
      if (holding.components) {
        console.log(`    包含: ${holding.components.length} 个组件`);
        holding.components.forEach(comp => {
          console.log(`      - ${comp.originalTicker}: ${comp.holding}股 @ ${comp.costPerShare} [${comp.exchange}]`);
        });
      }
    });
    
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
  node csvToFirebase.js <csv文件路径> [账户ID]
  node csvToFirebase.js --dir <目录路径>
  node csvToFirebase.js --show <账户ID>

示例:
  node csvToFirebase.js IB7075_20251008.csv
  node csvToFirebase.js --dir ./csv_files
  node csvToFirebase.js --show IB7075
    `);
    return;
  }

  try {
    if (args[0] === '--dir' && args[1]) {
      await processCsvDirectory(args[1]);
    } else if (args[0] === '--show' && args[1]) {
      await displayAccountData(args[1]);
    } else if (args[0] && args[1]) {
      await processCsvToFirebase(args[0], args[1]);
    } else if (args[0]) {
      await processCsvToFirebase(args[0]);
    }
  } catch (error) {
    console.error('❌ 执行失败:', error.message);
  }
}

// 运行主函数
main().catch(console.error);

// 优雅关闭
process.on('SIGINT', () => {
  console.log('\n🛑 正在关闭...');
  admin.app().delete().then(() => {
    console.log('✅ Firebase连接已关闭');
    process.exit(0);
  });
});

module.exports = {
  processCsvToFirebase,
  processCsvDirectory,
  sanitizeTicker,
  extractAccountIdFromFilename,
  isUSTreasury,
  mergeUSTreasuryHoldings,
  getExchangeFullName,
  inferExchangeFromTicker
};
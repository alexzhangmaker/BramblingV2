// toolImportIBCSV.js (修复版本)
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const csv = require('csv-parser');
const readline = require('readline');

class IBImportTool {
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
    
    // AssetClass 映射配置
    this.assetClassMapping = {
      'STK': 'equity',
      'ETF': 'etf',
      'FUND': 'fund',
      'BOND': 'bond',
      'OPT': 'option',
      'FUT': 'future',
      'CASH': 'cash',
      'CFD': 'cfd'
    };

    // 交易所映射配置
    this.exchangeMapping = {
      'NASDAQ': 'US',
      'NYSE': 'US',
      'ARCA': 'US',
      'AMEX': 'US',
      'SEHK': 'HK',
      'SHSE': 'CN',
      'SZSE': 'CN',
      'LSE': 'UK',
      'TSE': 'CA'
    };
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
    console.log('🔐 初始化 IB CSV 导入工具...');
    
    try {
      // 测试连接
      const { error: testError } = await this.supabase
        .from('account_IB7075')
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

  // 主导入函数
  async importFromCSV(csvFilePath, targetAccountTable) {
    console.log(`\n📁 开始导入 CSV 文件: ${csvFilePath}`);
    console.log(`🎯 目标表: ${targetAccountTable}`);
    
    const startTime = Date.now();

    try {
      // 验证目标表是否存在
      if (!await this.validateTargetTable(targetAccountTable)) {
        throw new Error(`目标表 ${targetAccountTable} 不存在或无法访问`);
      }

      // 读取并解析 CSV 文件
      const csvData = await this.parseCSVFile(csvFilePath);
      
      if (!csvData || csvData.length === 0) {
        throw new Error('CSV 文件为空或解析失败');
      }

      console.log(`✅ 成功解析 CSV 文件，共 ${csvData.length} 条记录`);

      // 转换数据格式
      const transformedData = this.transformData(csvData, targetAccountTable);
      
      if (transformedData.length === 0) {
        throw new Error('数据转换后没有有效记录');
      }

      console.log(`✅ 数据转换完成，共 ${transformedData.length} 条有效记录`);

      // 导入数据到数据库
      const importResult = await this.importToDatabase(transformedData, targetAccountTable);
      
      const duration = Date.now() - startTime;
      
      console.log(`\n🎉 CSV 导入完成!`);
      console.log(`   文件: ${csvFilePath}`);
      console.log(`   目标表: ${targetAccountTable}`);
      console.log(`   处理记录: ${transformedData.length} 条`);
      console.log(`   成功导入: ${importResult.success} 条`);
      console.log(`   导入失败: ${importResult.failed} 条`);
      console.log(`   耗时: ${duration}ms`);
      
      return {
        success: true,
        file: csvFilePath,
        targetTable: targetAccountTable,
        recordsProcessed: transformedData.length,
        recordsImported: importResult.success,
        recordsFailed: importResult.failed,
        duration: duration,
        details: importResult.details
      };
      
    } catch (error) {
      const duration = Date.now() - startTime;
      console.error(`❌ CSV 导入失败:`, error.message);
      
      return {
        success: false,
        file: csvFilePath,
        targetTable: targetAccountTable,
        error: error.message,
        duration: duration
      };
    }
  }

  // 验证目标表是否存在
  async validateTargetTable(tableName) {
    try {
      const { error } = await this.supabase
        .from(tableName)
        .select('count')
        .limit(1);

      if (error && error.code === '42P01') { // 表不存在
        return false;
      }
      
      return true;
    } catch (error) {
      console.error(`❌ 验证表 ${tableName} 失败:`, error.message);
      return false;
    }
  }

  // 解析 CSV 文件
  parseCSVFile(filePath) {
    return new Promise((resolve, reject) => {
      if (!fs.existsSync(filePath)) {
        reject(new Error(`CSV 文件不存在: ${filePath}`));
        return;
      }

      const results = [];
      
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (data) => {
          // 过滤空行和无效数据
          if (data.Symbol && data.Symbol.trim() !== '') {
            results.push(data);
          }
        })
        .on('end', () => {
          resolve(results);
        })
        .on('error', (error) => {
          reject(error);
        });
    });
  }

  // 转换数据格式（修复版本 - 移除 _original 字段）
  transformData(csvData, targetAccountTable) {
    const transformed = [];
    const seenTickers = new Set(); // 用于去重

    csvData.forEach((record, index) => {
      try {
        // 基本字段映射
        const ticker = record.Symbol ? record.Symbol.trim() : null;
        const company = record.Description ? record.Description.trim() : null;
        const quantity = parseFloat(record.Quantity) || 0;
        const costPrice = parseFloat(record.CostBasisPrice) || 0;

        // 跳过无效记录
        if (!ticker || quantity <= 0) {
          console.log(`   ⚠️ 跳过无效记录 [${index}]: ${ticker || '无代码'}, 数量: ${quantity}`);
          return;
        }

        // 去重检查
        if (seenTickers.has(ticker)) {
          console.log(`   ⚠️ 跳过重复代码: ${ticker}`);
          return;
        }
        seenTickers.add(ticker);

        // 转换 ClientAccountID
        const accountID = this.transformAccountID(record.ClientAccountID, targetAccountTable);
        
        // 转换 AssetClass
        const quoteType = this.transformAssetClass(record.AssetClass);
        
        // 转换交易所
        const exchange = this.transformExchange(record.ListingExchange);
        
        // 转换货币
        const currency = record.CurrencyPrimary ? record.CurrencyPrimary.toUpperCase() : 'USD';

        // 计算 CNY 成本（如果有汇率）
        const fxRate = parseFloat(record.FXRateToBase) || 1;
        const costCNY = costPrice * fxRate;

        // 只包含数据库表中实际存在的字段
        const transformedRecord = {
          ticker: ticker,
          company: company || `${ticker} Company`,
          holding: Math.round(quantity), // 持仓数量取整
          costPerShare: parseFloat(costPrice.toFixed(4)),
          currency: currency,
          accountID: accountID,
          quoteType: quoteType,
          exchange: exchange,
          CostCNY: parseFloat(costCNY.toFixed(4)),
          exchangeRate: parseFloat(fxRate.toFixed(6))
        };

        transformed.push(transformedRecord);
        
        console.log(`   ✅ 转换记录 [${index}]: ${ticker} -> ${accountID}, ${quantity}股 @ $${costPrice}`);
        
      } catch (error) {
        console.log(`   ❌ 转换记录 [${index}] 失败:`, error.message);
      }
    });

    return transformed;
  }

  // 转换 ClientAccountID
  transformAccountID(clientAccountID, targetAccountTable) {
    if (!clientAccountID) {
      // 从目标表名提取账户ID
      const match = targetAccountTable.match(/account_(IB\d+)/);
      return match ? match[1] : 'IB0000';
    }
    
    // 提取后四位数字
    const digits = clientAccountID.match(/\d+/g);
    if (digits && digits.length > 0) {
      const lastDigits = digits[digits.length - 1];
      const accountNumber = lastDigits.slice(-4); // 取最后4位
      return `IB${accountNumber}`;
    }
    
    // 如果无法提取，使用默认值
    return 'IB0000';
  }

  // 转换 AssetClass
  transformAssetClass(assetClass) {
    if (!assetClass) return 'equity';
    
    const normalized = assetClass.trim().toUpperCase();
    return this.assetClassMapping[normalized] || 'equity';
  }

  // 转换交易所
  transformExchange(listingExchange) {
    if (!listingExchange) return 'US';
    
    const normalized = listingExchange.trim().toUpperCase();
    return this.exchangeMapping[normalized] || 'US';
  }

  // 导入数据到数据库（修复版本 - 只插入有效字段）
  async importToDatabase(transformedData, targetAccountTable) {
    const results = {
      success: 0,
      failed: 0,
      details: []
    };

    for (const record of transformedData) {
      try {
        // 创建只包含有效字段的副本
        const cleanRecord = { ...record };
        
        // 使用 upsert 操作（存在则更新，不存在则插入）
        const { data, error } = await this.supabase
          .from(targetAccountTable)
          .upsert(cleanRecord, {
            onConflict: 'ticker',
            ignoreDuplicates: false
          })
          .select();

        if (error) {
          console.log(`   ❌ 导入 ${record.ticker} 失败:`, error.message);
          results.failed++;
          results.details.push({
            ticker: record.ticker,
            status: 'error',
            error: error.message
          });
        } else {
          console.log(`   ✅ 导入 ${record.ticker} 成功`);
          results.success++;
          results.details.push({
            ticker: record.ticker,
            status: 'success',
            action: data && data[0] ? 'updated' : 'inserted'
          });
        }
        
        // 添加小延迟避免过快请求
        await new Promise(resolve => setTimeout(resolve, 50));
        
      } catch (error) {
        console.log(`   ❌ 导入 ${record.ticker} 时异常:`, error.message);
        results.failed++;
        results.details.push({
          ticker: record.ticker,
          status: 'error',
          error: error.message
        });
      }
    }

    return results;
  }

  // 显示映射配置
  showMappings() {
    console.log('\n🔄 字段映射配置:');
    console.log('=' .repeat(40));
    
    console.log('CSV 字段 -> 数据库字段:');
    console.log('  Symbol -> ticker');
    console.log('  Description -> company');
    console.log('  Quantity -> holding');
    console.log('  CostBasisPrice -> costPerShare');
    console.log('  CurrencyPrimary -> currency');
    console.log('  ClientAccountID -> accountID (转换后)');
    console.log('  AssetClass -> quoteType (映射后)');
    console.log('  ListingExchange -> exchange (映射后)');
    
    console.log('\nAssetClass 映射:');
    Object.entries(this.assetClassMapping).forEach(([from, to]) => {
      console.log(`  ${from} -> ${to}`);
    });
    
    console.log('\n交易所映射:');
    Object.entries(this.exchangeMapping).forEach(([from, to]) => {
      console.log(`  ${from} -> ${to}`);
    });
  }

  // 预览 CSV 数据（不实际导入）
  async previewCSV(csvFilePath) {
    console.log(`\n👀 预览 CSV 文件: ${csvFilePath}`);
    
    try {
      const csvData = await this.parseCSVFile(csvFilePath);
      
      if (!csvData || csvData.length === 0) {
        console.log('❌ CSV 文件为空');
        return;
      }

      console.log(`📊 文件包含 ${csvData.length} 条记录`);
      console.log('\n前5条记录样例:');
      
      csvData.slice(0, 5).forEach((record, index) => {
        console.log(`\n[记录 ${index + 1}]`);
        console.log(`  Symbol: ${record.Symbol}`);
        console.log(`  Description: ${record.Description}`);
        console.log(`  Quantity: ${record.Quantity}`);
        console.log(`  CostBasisPrice: ${record.CostBasisPrice}`);
        console.log(`  ClientAccountID: ${record.ClientAccountID}`);
        console.log(`  AssetClass: ${record.AssetClass}`);
        console.log(`  ListingExchange: ${record.ListingExchange}`);
        console.log(`  CurrencyPrimary: ${record.CurrencyPrimary}`);
      });

      // 显示统计信息
      const tickers = new Set();
      const assetClasses = new Set();
      const exchanges = new Set();
      
      csvData.forEach(record => {
        if (record.Symbol) tickers.add(record.Symbol);
        if (record.AssetClass) assetClasses.add(record.AssetClass);
        if (record.ListingExchange) exchanges.add(record.ListingExchange);
      });

      console.log('\n📈 统计信息:');
      console.log(`  唯一代码: ${tickers.size} 个`);
      console.log(`  资产类型: ${Array.from(assetClasses).join(', ')}`);
      console.log(`  交易所: ${Array.from(exchanges).join(', ')}`);
      
    } catch (error) {
      console.error('❌ 预览失败:', error.message);
    }
  }

  // 新增：检查表结构的方法
  async checkTableStructure(tableName) {
    try {
      console.log(`\n🔍 检查表结构: ${tableName}`);
      
      const { data, error } = await this.supabase
        .from(tableName)
        .select('*')
        .limit(1);

      if (error) {
        console.log(`❌ 检查表结构失败:`, error.message);
        return null;
      }

      if (data && data.length > 0) {
        console.log('✅ 表字段:');
        Object.keys(data[0]).forEach(field => {
          console.log(`  - ${field}`);
        });
        return Object.keys(data[0]);
      } else {
        console.log('ℹ️ 表为空，无法获取字段信息');
        return null;
      }
      
    } catch (error) {
      console.error('❌ 检查表结构异常:', error.message);
      return null;
    }
  }
}

// 创建命令行交互界面
function createInterface() {
  return readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });
}

// 主函数 - 命令行交互模式
async function main() {
  console.log('🚀 IB CSV 导入工具 (修复版本)');
  console.log('=' .repeat(50));
  
  const tool = new IBImportTool();
  const initialized = await tool.initialize();
  
  if (!initialized) {
    console.log('❌ 初始化失败，程序退出');
    return;
  }

  // 显示映射配置
  tool.showMappings();

  const rl = createInterface();
  
  function askQuestion(question) {
    return new Promise((resolve) => {
      rl.question(question, resolve);
    });
  }

  try {
    // 获取 CSV 文件路径
    const csvFilePath = await askQuestion('\n📁 请输入 CSV 文件路径: ');
    
    if (!fs.existsSync(csvFilePath)) {
      console.log('❌ 文件不存在，请检查路径');
      rl.close();
      return;
    }

    // 预览文件
    await tool.previewCSV(csvFilePath);

    // 获取目标表名
    const targetTable = await askQuestion('\n🎯 请输入目标账户表名 (如 account_IB7075): ');
    
    // 检查表结构（可选）
    const checkStructure = await askQuestion('\n🔍 是否检查表结构? (y/N): ');
    if (checkStructure.toLowerCase() === 'y') {
      await tool.checkTableStructure(targetTable);
    }

    // 确认操作
    const confirm = await askQuestion(`\n⚠️  确认将 ${csvFilePath} 导入到 ${targetTable}? (y/N): `);
    
    if (confirm.toLowerCase() !== 'y') {
      console.log('❌ 操作已取消');
      rl.close();
      return;
    }

    // 执行导入
    console.log('\n🔄 开始导入...');
    const result = await tool.importFromCSV(csvFilePath, targetTable);
    
    if (result.success) {
      console.log('\n🎉 导入成功完成!');
    } else {
      console.log('\n❌ 导入失败:', result.error);
    }
    
  } catch (error) {
    console.error('💥 程序执行异常:', error.message);
  } finally {
    rl.close();
  }
}

// 直接导入函数（供其他脚本调用）
async function importCSVDirectly(csvFilePath, targetAccountTable) {
  const tool = new IBImportTool();
  await tool.initialize();
  
  console.log(`🚀 直接导入: ${csvFilePath} -> ${targetAccountTable}`);
  const result = await tool.importFromCSV(csvFilePath, targetAccountTable);
  
  return result;
}

// 导出模块
module.exports = {
  IBImportTool,
  importCSVDirectly,
  main
};

// 如果直接运行此文件，启动交互模式
if (require.main === module) {
  // 检查是否有命令行参数
  if (process.argv.length >= 4) {
    // 命令行模式: node toolImportIBCSV.js <csv文件> <目标表>
    const csvFile = process.argv[2];
    const targetTable = process.argv[3];
    
    importCSVDirectly(csvFile, targetTable)
      .then(result => {
        if (result.success) {
          console.log('✅ 导入成功');
          process.exit(0);
        } else {
          console.log('❌ 导入失败');
          process.exit(1);
        }
      })
      .catch(error => {
        console.error('💥 程序异常:', error);
        process.exit(1);
      });
  } else {
    // 交互模式
    main().catch(console.error);
  }
}
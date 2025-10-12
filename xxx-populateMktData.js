// populateMktData.js
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const yAPIModule = require("./YFinanceAPI.js") ;

// 初始化 Supabase 客户端
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseKey) {
    throw new Error('缺少必要的环境变量: SUPABASE_URL 和 SUPABASE_ANON_KEY');
}

let supabase = createClient(supabaseUrl, supabaseKey);

/**
 * 使用服务账户登录获取认证客户端
 */
async function getAuthenticatedClient() {
    try {
        console.log('正在使用服务账户登录...');
        const { data, error } = await supabase.auth.signInWithPassword({
            email: process.env.SERVICE_ACCOUNT_EMAIL,
            password: process.env.SERVICE_ACCOUNT_PASSWORD,
        });
        
        if (error) {
            throw new Error(`服务账户登录失败: ${error.message}`);
        }
        
        console.log('✅ 服务账户登录成功');
        
        // 创建新的认证客户端
        const authClient = createClient(supabaseUrl, supabaseKey, {
            global: {
                headers: {
                    Authorization: `Bearer ${data.session.access_token}`
                }
            }
        });
        
        return authClient;
    } catch (error) {
        console.error('服务账户认证时发生错误:', error);
        throw error;
    }
}

/**
 * 获取认证的 Supabase 客户端
 */
async function getAuthSupabase() {
    try {
        const authClient = await getAuthenticatedClient();
        return authClient;
    } catch (error) {
        console.error('获取认证客户端失败:', error);
        // 回退到普通客户端
        return supabase;
    }
}

/**
 * 填充汇率数据
 * @param {string} from - 源货币代码 (如: USD)
 * @param {string} to - 目标货币代码 (如: EUR)
 * @returns {Promise<Object>} 返回操作结果
 */
async function populateExchangeRate(from, to) {
    try {
        console.log(`开始处理汇率: ${from} -> ${to}`);
        
        // 使用认证客户端
        const authSupabase = await getAuthSupabase();
        
        // 获取当前日期
        const today = new Date().toISOString().split('T')[0];
        
        // 检查今天是否已有汇率记录
        const existingRate = await checkExchangeRateExists(from, to, today, authSupabase);
        
        if (existingRate) {
            console.log(`汇率 ${from}->${to} 今天已有记录，跳过更新`);
            return {
                success: true,
                action: 'skipped',
                from: from,
                to: to,
                rate: existingRate.rate,
                message: '今天已有汇率记录'
            };
        }
        
        // 调用API获取汇率
        const exchangeRate = await yAPIModule.API_FetchExRate(from, to);
        
        if (exchangeRate === null || exchangeRate === undefined) {
            throw new Error(`无法获取汇率 ${from} -> ${to}`);
        }
        
        // 插入或更新汇率记录
        const rateResult = await insertExchangeRate(from, to, exchangeRate, today, authSupabase);
        
        return {
            success: true,
            action: 'updated',
            rateId: rateResult.id,
            from: from,
            to: to,
            rate: exchangeRate,
            rateDate: today
        };
    } catch (error) {
        console.error(`处理汇率 ${from}->${to} 时发生错误:`, error);
        throw error;
    }
}

/**
 * 检查汇率记录是否已存在
 * @param {string} from - 源货币
 * @param {string} to - 目标货币
 * @param {string} rateDate - 汇率日期
 * @param {Object} client - Supabase 客户端
 * @returns {Promise<Object|null>} 返回汇率记录或null
 */
async function checkExchangeRateExists(from, to, rateDate, client = supabase) {
    try {
        const { data, error } = await client
            .from('exchange_rates')
            .select('id, rate')
            .eq('from_currency', from)
            .eq('to_currency', to)
            .eq('rate_date', rateDate)
            .maybeSingle();
        
        if (error) {
            throw new Error(`Supabase 查询错误: ${error.message}`);
        }
        
        return data;
    } catch (error) {
        console.error('检查汇率存在性时发生错误:', error);
        throw error;
    }
}

/**
 * 插入或更新汇率记录
 * @param {string} from - 源货币
 * @param {string} to - 目标货币
 * @param {number} rate - 汇率
 * @param {string} rateDate - 汇率日期
 * @param {Object} client - Supabase 客户端
 * @returns {Promise<Object>} 返回插入结果
 */
async function insertExchangeRate(from, to, rate, rateDate, client = supabase) {
    try {
        const { data, error } = await client
            .from('exchange_rates')
            .upsert({
                from_currency: from,
                to_currency: to,
                rate: rate,
                rate_date: rateDate,
                source: 'API',
                created_at: new Date().toISOString()
            }, {
                onConflict: 'from_currency,to_currency,rate_date'
            })
            .select('id')
            .single();
        
        if (error) {
            throw new Error(`插入汇率记录失败: ${error.message}`);
        }
        
        return { id: data.id };
    } catch (error) {
        console.error('插入汇率记录时发生错误:', error);
        throw error;
    }
}

/**
 * 批量处理多个汇率对
 * @param {Array} ratePairs - 汇率对数组，可以是字符串或对象 {from, to}
 * @returns {Promise<Array>} 返回所有操作的结果
 */
async function populateExchangeRatesBatch(ratePairs) {
    try {
        const results = [];
        
        // 获取认证客户端（在整个批量过程中使用同一个认证会话）
        const authSupabase = await getAuthSupabase();
        
        for (const pair of ratePairs) {
            try {
                let from, to;
                
                if (typeof pair === 'string') {
                    // 如果是字符串格式 "USD/EUR"
                    const parts = pair.split('/');
                    if (parts.length !== 2) {
                        throw new Error('无效的汇率对格式，请使用 "USD/EUR" 格式');
                    }
                    from = parts[0].toUpperCase();
                    to = parts[1].toUpperCase();
                } else if (typeof pair === 'object' && pair.from && pair.to) {
                    from = pair.from.toUpperCase();
                    to = pair.to.toUpperCase();
                } else {
                    console.error('无效的汇率对格式:', pair);
                    results.push({
                        success: false,
                        from: 'unknown',
                        to: 'unknown',
                        error: '无效的汇率对格式'
                    });
                    continue;
                }
                
                const result = await populateExchangeRate(from, to);
                results.push(result);
                
                // 添加延迟以避免API限制
                await new Promise(resolve => setTimeout(resolve, 100));
            } catch (error) {
                console.error(`处理汇率对 ${pair} 时发生错误:`, error);
                const from = typeof pair === 'string' ? pair.split('/')[0] : (pair.from || 'unknown');
                const to = typeof pair === 'string' ? pair.split('/')[1] : (pair.to || 'unknown');
                results.push({
                    success: false,
                    from: from,
                    to: to,
                    error: error.message
                });
            }
        }
        
        return results;
    } catch (error) {
        console.error('批量处理汇率对时发生错误:', error);
        throw error;
    }
}

/**
 * 获取汇率历史
 * @param {string} from - 源货币
 * @param {string} to - 目标货币
 * @param {number} days - 天数
 * @returns {Promise<Array>} 返回汇率历史
 */
async function getExchangeRateHistory(from, to, days = 30) {
    try {
        const authSupabase = await getAuthSupabase();
        
        const startDate = new Date();
        startDate.setDate(startDate.getDate() - days);
        
        const { data, error } = await authSupabase
            .from('exchange_rates')
            .select('rate, rate_date, source')
            .eq('from_currency', from)
            .eq('to_currency', to)
            .gte('rate_date', startDate.toISOString().split('T')[0])
            .order('rate_date', { ascending: true });
        
        if (error) {
            throw new Error(`获取汇率历史失败: ${error.message}`);
        }
        
        return data;
    } catch (error) {
        console.error('获取汇率历史时发生错误:', error);
        throw error;
    }
}

/**
 * 获取最新汇率
 * @param {string} from - 源货币
 * @param {string} to - 目标货币
 * @returns {Promise<Object|null>} 返回最新汇率
 */
async function getLatestExchangeRate(from, to) {
    try {
        const authSupabase = await getAuthSupabase();
        
        const { data, error } = await authSupabase
            .from('exchange_rates')
            .select('rate, rate_date, source')
            .eq('from_currency', from)
            .eq('to_currency', to)
            .order('rate_date', { ascending: false })
            .limit(1)
            .single();
        
        if (error) {
            if (error.code === 'PGRST116') { // 没有找到记录
                return null;
            }
            throw new Error(`获取最新汇率失败: ${error.message}`);
        }
        
        return data;
    } catch (error) {
        console.error('获取最新汇率时发生错误:', error);
        throw error;
    }
}

// 以下是原有的股票相关函数（保持原有实现，但添加了 client 参数支持）
// ... [原有的 populateQuote, checkAssetExists, createNewAssetAndPrice 等函数保持不变]
// 只需要确保这些函数都接受 client 参数

/**
 * 填充市场数据的主要函数
 */
async function populateQuote(ticker, exchange = null) {
    try {
        console.log(`开始处理股票代码: ${ticker}, 交易所: ${exchange || '默认'}`);
        
        // 使用认证客户端
        const authSupabase = await getAuthSupabase();
        
        // 1. 检查资产是否已存在
        const existingAsset = await checkAssetExists(ticker, exchange, authSupabase);
        
        if (!existingAsset) {
            console.log(`资产 ${ticker} 不存在，创建新记录`);
            return await createNewAssetAndPrice(ticker, exchange, authSupabase);
        } else {
            console.log(`资产 ${ticker} 已存在，ID: ${existingAsset.id}`);
            return await updateAssetPrice(existingAsset.id, ticker, authSupabase);
        }
    } catch (error) {
        console.error(`处理股票代码 ${ticker} 时发生错误:`, error);
        throw error;
    }
}

/**
 * 检查资产是否已存在
 */
async function checkAssetExists(symbol, exchange, client = supabase) {
    try {
        let query = client
            .from('assets')
            .select('id, symbol, name, asset_type, currency, exchange')
            .eq('symbol', symbol);
        
        if (exchange) {
            query = query.eq('exchange', exchange);
        } else {
            query = query.is('exchange', null);
        }
        
        const { data, error } = await query;
        
        if (error) {
            throw new Error(`Supabase 查询错误: ${error.message}`);
        }
        
        return data && data.length > 0 ? data[0] : null;
    } catch (error) {
        console.error('检查资产存在性时发生错误:', error);
        throw error;
    }
}

/**
 * 创建新资产和价格记录
 */
async function createNewAssetAndPrice(symbol, exchange, client = supabase) {
    try {
        // 调用API获取股票元数据
        const stockMeta = await yAPIModule.API_FetchStockMeta(symbol, exchange);
        
        if (!stockMeta) {
            throw new Error(`无法获取股票 ${symbol} 的元数据`);
        }
        
        // 插入资产记录
        const assetId = await insertAsset(stockMeta, client);
        
        // 插入价格记录
        const priceResult = await insertAssetPrice(assetId, stockMeta.quoteTTM, stockMeta.currency, null, client);
        
        return {
            success: true,
            action: 'created',
            assetId: assetId,
            priceId: priceResult.id,
            symbol: stockMeta.symbol,
            name: stockMeta.name,
            price: stockMeta.quoteTTM,
            currency: stockMeta.currency
        };
    } catch (error) {
        console.error('创建新资产和价格时发生错误:', error);
        throw error;
    }
}

/**
 * 插入资产记录
 */
async function insertAsset(stockMeta, client = supabase) {
    try {
        const { data, error } = await client
            .from('assets')
            .insert({
                symbol: stockMeta.symbol,
                name: stockMeta.name,
                asset_type: stockMeta.asset_type,
                currency: stockMeta.currency,
                exchange: stockMeta.exchange,
                sector: stockMeta.sector || null,
                isin: stockMeta.isin || null,
                cusip: stockMeta.cusip || null,
                created_at: new Date().toISOString()
            })
            .select('id')
            .single();
        
        if (error) {
            if (error.code === '23505') {
                console.log('资产已存在，尝试重新查询');
                const existingAsset = await checkAssetExists(stockMeta.symbol, stockMeta.exchange, client);
                if (existingAsset) {
                    return existingAsset.id;
                }
            }
            throw new Error(`插入资产记录失败: ${error.message}`);
        }
        
        return data.id;
    } catch (error) {
        console.error('插入资产记录时发生错误:', error);
        throw error;
    }
}

/**
 * 插入资产价格记录
 */
async function insertAssetPrice(assetId, price, currency, priceDate = null, client = supabase) {
    try {
        const dateToUse = priceDate || new Date().toISOString().split('T')[0];
        
        const { data, error } = await client
            .from('asset_prices')
            .upsert({
                asset_id: assetId,
                price: price,
                currency: currency,
                price_date: dateToUse,
                source: 'API',
                created_at: new Date().toISOString()
            }, {
                onConflict: 'asset_id,price_date'
            })
            .select('id')
            .single();
        
        if (error) {
            throw new Error(`插入资产价格记录失败: ${error.message}`);
        }
        
        return { id: data.id };
    } catch (error) {
        console.error('插入资产价格记录时发生错误:', error);
        throw error;
    }
}

// ... [其他原有函数保持不变]

// 测试函数
async function testConnection() {
    try {
        console.log('测试 Supabase 连接...');
        const authSupabase = await getAuthSupabase();
        const { data, error } = await authSupabase.from('assets').select('count').limit(1);
        
        if (error) {
            throw error;
        }
        
        console.log('✅ Supabase 连接成功');
        return true;
    } catch (error) {
        console.error('❌ Supabase 连接失败:', error.message);
        return false;
    }
}

// 如果直接运行此文件，执行测试
if (require.main === module) {
    async function main() {
        try {
            const isConnected = await testConnection();
            if (!isConnected) {
                process.exit(1);
            }
            
            console.log('✅ populateMktData.js 加载成功，可以正常使用');
            
            // 测试汇率功能
            console.log('测试汇率功能...');
            const rateResult = await populateExchangeRate('USD', 'CNY');
            console.log('汇率测试结果:', rateResult);
            
            // 测试批量汇率
            const ratePairs = ['USD/EUR', 'USD/JPY', { from: 'EUR', to: 'GBP' }];
            const batchRateResults = await populateExchangeRatesBatch(ratePairs);
            console.log('批量汇率测试结果:', batchRateResults);
            
        } catch (error) {
            console.error('主程序执行错误:', error);
            process.exit(1);
        }
    }
    
    main();
}

// CommonJS 导出
module.exports = {
    // 汇率相关函数
    populateExchangeRate,
    populateExchangeRatesBatch,
    getExchangeRateHistory,
    getLatestExchangeRate,
    
    // 股票相关函数
    populateQuote,
    populateQuotesBatch,
    getAuthenticatedClient,
    checkAssetExists,
    createNewAssetAndPrice,
    updateAssetPrice,
    getPriceHistory,
    testConnection,
    supabase
};

module.exports.default = populateQuote;
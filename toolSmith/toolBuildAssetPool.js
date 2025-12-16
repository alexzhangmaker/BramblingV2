// toolBuildAssetPool.js
const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');

// 初始化Firebase
const serviceKeyPath = '/Users/zhangqing/Documents/Github/serviceKeys/bramblingV2Firebase.json';

if (!fs.existsSync(serviceKeyPath)) {
    console.error(`❌ 找不到 Service Account Key 文件: ${serviceKeyPath}`);
    process.exit(1);
}

const serviceAccount = require(serviceKeyPath);

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: 'https://outpost-8d74e-14018.firebaseio.com/'
});

const db = admin.database();

/**
 * 根据Ticker和Exchange推断国家/地区
 */
function inferCountry(ticker, exchange) {
    if (!ticker) return 'Unknown';

    const upperTicker = ticker.toUpperCase();

    if (upperTicker.endsWith('.HK')) return 'HK';
    if (upperTicker.endsWith('.SS') || upperTicker.endsWith('.SZ')) return 'CN';
    if (upperTicker.endsWith('.L')) return 'UK';
    if (upperTicker.endsWith('.TO') || upperTicker.endsWith('.V')) return 'CA';
    if (upperTicker.endsWith('.DE')) return 'DE';
    if (upperTicker.endsWith('.PA')) return 'FR';
    if (upperTicker.endsWith('.AS')) return 'NL';

    // 如果没有后缀，通常假设是美股，或者根据 exchange 判断
    if (['US', 'NYSE', 'NASDAQ', 'AMEX'].includes(exchange?.toUpperCase())) return 'US';

    // 默认判定为US (如果没有明显后缀)
    if (!upperTicker.includes('.')) return 'US';

    return 'Unknown';
}

/**
 * 标准化 Yahoo Finance Ticker 格式
 * 规则：
 * 1. 转大写
 * 2. 处理特殊股 (e.g. BRK B -> BRK-B)
 * 3. 移除多余空格
 */
function normalizeTicker(rawTicker) {
    if (!rawTicker) return '';
    let ticker = rawTicker.trim().toUpperCase();

    // 处理美股双重股权结构，Yahoo使用连字符 (e.g. BRK-B, BF-B)
    // 常见输入可能是 "BRK B", "BRK.B", "BRK/B"
    if (ticker.includes(' ')) {
        ticker = ticker.replace(/\s+/g, '-');
    }

    // 一些特殊的特定修正（参考自 svcUpdateQuote.js）
    if (ticker === 'BF B') ticker = 'BF-B';
    if (ticker === 'BRK B') ticker = 'BRK-B';

    return ticker;
}

/**
 * Firebase Key 不能包含 ., #, $, [, ]
 * 通常我们将 . 替换为 _
 */
function sanitizeKey(key) {
    return key.replace(/\./g, '_').replace(/[#$\[\]]/g, '_');
}

/**
 * 构建资产池
 */
async function buildAssetPool() {
    console.log('🚀 开始构建 Asset Pool ...');

    try {
        // 1. 获取现有账户数据
        const accountsSnapshot = await db.ref('accounts').once('value');
        const accounts = accountsSnapshot.val();

        if (!accounts) {
            console.log('⚠️ 未找到任何账户数据');
            return;
        }

        // 1.1 获取现有 Asset Pool 数据 (用于判断状态)
        const assetPoolSnapshot = await db.ref('assetPool').once('value');
        const existingAssetPool = assetPoolSnapshot.val() || {};

        const assetMap = new Map();
        let totalHoldingsProcessed = 0;

        // 2. 遍历所有持仓，聚合唯一资产
        for (const [accountId, accountData] of Object.entries(accounts)) {
            if (!accountData.holdings) continue;

            for (const [holdingId, holdingData] of Object.entries(accountData.holdings)) {
                totalHoldingsProcessed++;

                const ticker = normalizeTicker(holdingData.ticker);
                if (!ticker) continue;

                // 收集元数据
                // 注意：后续账户的同一Ticker数据会覆盖前面的 (在这个简单的构建逻辑中)
                // 理想情况下应该有一个"最佳数据源"选择逻辑，但这里我们假设最近处理的有效

                const existingAsset = assetMap.get(ticker);

                // 如果已存在，我们优先保留信息更全的那个（简单的非空覆盖策略）
                const mergedAsset = existingAsset ? { ...existingAsset } : {};

                // 提取需要的字段
                const company = holdingData.company || mergedAsset.company || '';
                const currency = holdingData.currency || mergedAsset.currency || '';
                const exchange = holdingData.exchange || mergedAsset.exchange || '';

                // 推断 Country
                const country = inferCountry(ticker, exchange);

                assetMap.set(ticker, {
                    ticker: ticker, // 保持原始 Yahoo 格式 (e.g. 0014.HK)
                    company,
                    country,
                    currency,
                    exchange
                });
            }
        }

        console.log(`📊 扫描了 ${Object.keys(accounts).length} 个账户，${totalHoldingsProcessed} 条持仓记录`);
        console.log(`🔍 识别出 ${assetMap.size} 个唯一资产`);

        // 3. 写入 assetPool
        if (assetMap.size > 0) {
            const updates = {};
            assetMap.forEach((data, ticker) => {
                const safeKey = sanitizeKey(ticker);

                // 逻辑: 如果是已经存在数据库中assetPool中的，那么这个字段为值为holding，新添加的默认为watching
                const isExisting = Object.prototype.hasOwnProperty.call(existingAssetPool, safeKey);
                data.status = isExisting ? 'holding' : 'watching';

                updates[safeKey] = data;
            });

            console.log('💾 正在写入 Firebase /assetPool ...');
            await db.ref('assetPool').update(updates);
            console.log('✅ Asset Pool 构建/更新完成！');
        }

    } catch (error) {
        console.error('❌ 构建过程出错:', error);
    } finally {
        process.exit(0);
    }
}

// 运行
if (require.main === module) {
    buildAssetPool();
}

module.exports = buildAssetPool;

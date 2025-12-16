// toolValidateFirebaseData.js
const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');

// 初始化Firebase
// 尝试从环境变量或硬编码路径加载 KEY
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
 * 比较两个对象在指定字段上是否一致
 */
function isConsistent(ref, current, fieldsToCheck) {
    const diffs = [];
    for (const field of fieldsToCheck) {
        // 简单的相等性检查，处理 null/undefined
        const val1 = ref[field] || '';
        const val2 = current[field] || '';
        if (val1 != val2) { // 使用宽松相等，允许 '1' == 1
            diffs.push({ field, val1, val2 });
        }
    }
    return diffs;
}

/**
 * 检查Ticker是否符合Yahoo Finance格式建议
 * 返回 { valid: boolean, suggestion: string }
 */
function checkTickerFormat(ticker) {
    if (!ticker) return { valid: false, suggestion: '' };

    let suggestion = ticker.trim().toUpperCase();

    // 检查1: 是否全大写 (且不包含中文等非ASCII字符，暂不严格限制非ASCII，主要关注大小写)
    const isUpperCase = ticker === ticker.toUpperCase();

    // 检查2: 是否包含空格 (Yahoo通常使用连字符)
    const hasSpace = ticker.includes(' ');

    if (hasSpace) {
        suggestion = suggestion.replace(/\s+/g, '-');
    }

    // 特殊修正
    if (suggestion === 'BF B') suggestion = 'BF-B';
    if (suggestion === 'BRK B') suggestion = 'BRK-B';

    const isValid = isUpperCase && !hasSpace;

    return { valid: isValid, suggestion: isValid ? '' : suggestion };
    return { valid: isValid, suggestion: isValid ? '' : suggestion };
}

/**
 * 检查交易所特定的 Ticker 规则
 * 返回 { valid: boolean, message: string }
 */
function checkExchangeSpecificRules(ticker, exchange) {
    if (!ticker || !exchange) return { valid: true }; // 无法检查，跳过

    const upTicker = ticker.toUpperCase();
    const upExchange = exchange.toUpperCase();

    // 规则1: HK 交易所 -> 必须以 .HK 结尾
    if (upExchange === 'HK' || upExchange === 'HKEX') {
        if (!upTicker.endsWith('.HK')) {
            return { valid: false, message: `Exchange is HK, but ticker '${ticker}' does not end with .HK` };
        }
    }

    // 规则2: CN 交易所
    if (upExchange === 'CN' || upExchange === 'SSE' || upExchange === 'SZSE') {
        // 必须以 6 或 0 开头
        const firstChar = upTicker.charAt(0);

        if (firstChar === '6') {
            if (!upTicker.endsWith('.SS')) {
                return { valid: false, message: `CN stock starting with '6' must end with .SS (got '${ticker}')` };
            }
        } else if (firstChar === '0') {
            if (!upTicker.endsWith('.SZ')) {
                return { valid: false, message: `CN stock starting with '0' must end with .SZ (got '${ticker}')` };
            }
        } else if (firstChar === '3') {
            // 创业板通常也是 .SZ
            if (!upTicker.endsWith('.SZ')) {
                return { valid: false, message: `CN stock starting with '3' must end with .SZ (got '${ticker}')` };
            }
        } else if (firstChar === '5') {
            if (!upTicker.endsWith('.SS')) {
                return { valid: false, message: `CN stock starting with '5' must end with .SS (got '${ticker}')` };
            }
        } else {
            // 暂时只按照用户要求检查 6 和 0，但也提示未知
            // 用户只说了: 必须是字符“6”或者“0”开头
            return { valid: false, message: `CN stock ticker '${ticker}' must start with '6' (.SS) or '0' (.SZ)` };
        }
    }

    // 规则3: LSE 交易所
    if (upExchange === 'LSE') {
        if (!upTicker.endsWith('.L')) {
            return { valid: false, message: `LSE stock ticker '${ticker}' must end with .L` };
        }
    }

    return { valid: true };
}

/**
 * 验证持仓数据一致性
 */
async function validateHoldingsConsistency() {
    console.log('🚀 开始验证 Holdings 数据一致性...');

    try {
        const snapshot = await db.ref('accounts').once('value');
        const accounts = snapshot.val();

        if (!accounts) {
            console.log('⚠️ 未找到任何账户数据');
            return;
        }

        const tickerMap = new Map(); // Key: ticker, Value: { refEntry: object, occurrences: [] }
        const inconsistencies = [];
        const formatIssues = []; // Ticker格式问题

        // 需要校验一致性的字段 (排除 holding, costPerShare, lastUpdated 等动态字段)
        const fieldsToCheck = [
            'company',
            'currency',
            'exchange',
            'exchangeCode',
            'assetClass',
            'description',
            'logo' // 如果有logo的话
        ];

        // 1. 遍历收集数据
        let totalHoldingsChecked = 0;

        for (const [accountId, accountData] of Object.entries(accounts)) {
            if (!accountData.holdings) continue;

            for (const [holdingId, holdingData] of Object.entries(accountData.holdings)) {
                totalHoldingsChecked++;
                const ticker = holdingData.ticker;

                // 检查 Ticker 格式
                const formatCheck = checkTickerFormat(ticker);
                if (!formatCheck.valid) {
                    formatIssues.push({
                        ticker,
                        accountId,
                        suggestion: formatCheck.suggestion
                    });
                }

                // 检查交易所特定规则
                const exchangeRuleCheck = checkExchangeSpecificRules(ticker, holdingData.exchange);
                if (!exchangeRuleCheck.valid) {
                    formatIssues.push({
                        ticker,
                        accountId,
                        suggestion: exchangeRuleCheck.message // 复用 suggestion 字段显示错误信息
                    });
                }

                if (!ticker) {
                    console.warn(`⚠️ 账户 ${accountId} 发现没有 Ticker 的持仓: ${holdingId}`);
                    continue;
                }

                if (!tickerMap.has(ticker)) {
                    // 记录第一个遇到的作为基准
                    tickerMap.set(ticker, {
                        refAccountId: accountId,
                        refData: holdingData,
                        occurrences: [{ accountId, holdingId }]
                    });
                } else {
                    // 后续遇到的，与基准进行对比
                    const record = tickerMap.get(ticker);
                    record.occurrences.push({ accountId, holdingId });

                    const diffs = isConsistent(record.refData, holdingData, fieldsToCheck);

                    if (diffs.length > 0) {
                        inconsistencies.push({
                            ticker,
                            accountId,
                            holdingId,
                            refAccountId: record.refAccountId,
                            diffs
                        });
                    }
                }
            }
        }

        // 2. 报告结果
        console.log(`\n📊 扫描完成`);
        console.log(`   检查账户数: ${Object.keys(accounts).length}`);
        console.log(`   检查持仓总数: ${totalHoldingsChecked}`);
        console.log(`   唯一 Ticker 数: ${tickerMap.size}`);

        if (inconsistencies.length === 0) {
            console.log('\n✅ 数据一致性检查通过：所有相同 Ticker 的元数据在不同账户间均保持一致。');
        } else {
            console.log(`\n❌ 发现 ${inconsistencies.length} 处数据不一致：`);
            console.log('='.repeat(80));

            // 按 Ticker 分组打印
            const groupedIssues = inconsistencies.reduce((acc, curr) => {
                if (!acc[curr.ticker]) acc[curr.ticker] = [];
                acc[curr.ticker].push(curr);
                return acc;
            }, {});

            for (const [ticker, issues] of Object.entries(groupedIssues)) {
                console.log(`🔹 Ticker: ${ticker}`);

                // 获取基准数据的信息
                const refRecord = tickerMap.get(ticker);
                console.log(`   基准来源: [${refRecord.refAccountId}]`);
                // 打印基准数据的重要字段值，便于对比
                const refInfo = fieldsToCheck.map(f => `${f}=${refRecord.refData[f] || '(empty)'}`).join(', ');
                console.log(`   基准数据: { ${refInfo} }`);

                issues.forEach(issue => {
                    console.log(`   ⚠️  不一致来源: [${issue.accountId}]`);
                    issue.diffs.forEach(diff => {
                        console.log(`      - 字段 [${diff.field}] 不匹配: 基准="${diff.val1}" vs 当前="${diff.val2}"`);
                    });
                });
                console.log('-'.repeat(40));
            }

            console.log('='.repeat(80));
            console.log('💡 建议：请检查上述账户的数据源，修正元数据以保持统一。');
        }

        // 3. 报告 Ticker 格式问题
        if (formatIssues.length > 0) {
            console.log('\n==================================================');
            console.log(`⚠️  发现 ${formatIssues.length} 个不符合 Yahoo Finance 规则的 Ticker`);
            console.log('==================================================');
            console.log('规则: 设置全大写，双重股权使用连字符(-)。例如: "tsla" ❌ -> "TSLA" ✅, "BRK B" ❌ -> "BRK-B" ✅');

            formatIssues.forEach(issue => {
                console.log(`❌ [${issue.ticker}] (账户: ${issue.accountId}) -> 建议: ${issue.suggestion}`);
            });
            console.log('==================================================');
        } else {
            console.log('\n✅ Ticker 格式检查通过。');
        }

    } catch (error) {
        console.error('❌ 验证过程出错:', error);
    } finally {
        process.exit(0);
    }
}

// 运行
if (require.main === module) {
    validateHoldingsConsistency();
}

module.exports = validateHoldingsConsistency;

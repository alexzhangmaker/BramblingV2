const admin = require('firebase-admin');
const duckdb = require('duckdb');
const path = require('path');
const fs = require('fs');

// 配置
const SERVICE_KEY_PATH = '/Users/zhangqing/Documents/Github/serviceKeys/bramblingV2Firebase.json';
const DUCK_DB_PATH = path.join(__dirname, '../duckDB/PortfolioData.duckdb');

// 初始化 Firebase
if (!fs.existsSync(SERVICE_KEY_PATH)) {
    console.error(`❌ 找不到 Service Account Key: ${SERVICE_KEY_PATH}`);
    process.exit(1);
}

const serviceAccount = require(SERVICE_KEY_PATH);
if (!admin.apps.length) {
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        databaseURL: 'https://outpost-8d74e-14018.firebaseio.com/'
    });
}
const db = admin.database();

// 初始化 DuckDB
class AuditTool {
    constructor() {
        this.dbInstance = new duckdb.Database(DUCK_DB_PATH);
    }

    createConnection() {
        const connection = this.dbInstance.connect();
        return connection;
    }

    closeConnection(connection) {
        if (connection) {
            try { connection.close(); } catch (e) { }
        }
    }

    query(connection, sql, params = []) {
        return new Promise((resolve, reject) => {
            connection.all(sql, ...params, (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });
    }

    /**
     * 1. 稽核同步层: Firebase -> DuckDB
     * @param {number} sampleSize 采样数量
     */
    async auditSync(sampleSize = 5) {
        console.log('\n🔍 [Sync Audit] 开始同步层稽核 (Firebase -> DuckDB)...');
        const connection = this.createConnection();
        const report = {
            totalAccounts: { firebase: 0, duckdb: 0, match: false },
            totalHoldings: { firebase: 0, duckdb: 0, match: false },
            samples: []
        };

        try {
            // 获取 Firebase 数据
            console.log('   正在读取 Firebase 数据...');
            const snapshot = await db.ref('accounts').once('value');
            const accounts = snapshot.val() || {};

            let fbHoldings = [];
            Object.entries(accounts).forEach(([accountId, accountData]) => {
                if (accountData.holdings) {
                    Object.entries(accountData.holdings).forEach(([_, holding]) => {
                        fbHoldings.push({
                            accountId,
                            ...holding
                        });
                    });
                }
            });

            report.totalAccounts.firebase = Object.keys(accounts).length;
            report.totalHoldings.firebase = fbHoldings.length;

            // 获取 DuckDB 数据统计
            console.log('   正在读取 DuckDB 统计...');
            const dbRef = await this.query(connection, `
                SELECT 
                    COUNT(DISTINCT accountID) as accountCount,
                    COUNT(*) as holdingCount 
                FROM tblAccountHoldings
            `);

            report.totalAccounts.duckdb = Number(dbRef[0].accountCount);
            report.totalHoldings.duckdb = Number(dbRef[0].holdingCount);

            report.totalAccounts.match = report.totalAccounts.firebase === report.totalAccounts.duckdb;
            report.totalHoldings.match = report.totalHoldings.firebase === report.totalHoldings.duckdb;

            // 采样比对
            console.log(`   正在进行采样比对 (样本数: ${sampleSize})...`);
            // 随机抽取样本
            const samples = [];
            if (fbHoldings.length > 0) {
                for (let i = 0; i < sampleSize; i++) {
                    const randomIndex = Math.floor(Math.random() * fbHoldings.length);
                    samples.push(fbHoldings[randomIndex]);
                }
            }

            for (const sample of samples) {
                const check = {
                    ticker: sample.ticker,
                    accountId: sample.accountId,
                    fields: {},
                    pass: true
                };

                const dbRows = await this.query(connection, `
                    SELECT * FROM tblAccountHoldings 
                    WHERE accountID = ? AND ticker = ?
                `, [sample.accountId, sample.ticker]);

                if (dbRows.length === 0) {
                    check.pass = false;
                    check.error = 'Not found in DuckDB';
                } else {
                    const dbRow = dbRows[0];
                    // 比对关键字段
                    const fields = ['holding', 'costPerShare', 'currency', 'assetClass'];
                    fields.forEach(field => {
                        let fbVal = sample[field];
                        let dbVal = dbRow[field];

                        // 简单的类型转换处理
                        if (typeof fbVal === 'number' && typeof dbVal === 'number') {
                            if (Math.abs(fbVal - dbVal) > 0.0001) {
                                check.fields[field] = { fb: fbVal, db: dbVal, match: false };
                                check.pass = false;
                            }
                        } else if (String(fbVal || '') !== String(dbVal || '')) {
                            check.fields[field] = { fb: fbVal, db: dbVal, match: false };
                            check.pass = false;
                        }
                    });
                }
                report.samples.push(check);
            }

            this.printSyncReport(report);

        } catch (error) {
            console.error('❌ Sync Audit 失败:', error);
        } finally {
            this.closeConnection(connection);
        }
    }

    printSyncReport(report) {
        console.log('   -------- 稽核报告 --------');
        console.log(`   账户总数: Firebase=${report.totalAccounts.firebase}, DuckDB=${report.totalAccounts.duckdb} [${report.totalAccounts.match ? '✅' : '❌'}]`);
        console.log(`   持仓总数: Firebase=${report.totalHoldings.firebase}, DuckDB=${report.totalHoldings.duckdb} [${report.totalHoldings.match ? '✅' : '❌'}]`);

        console.log('\n   采样比对结果:');
        report.samples.forEach(s => {
            if (s.pass) {
                console.log(`   ✅ [${s.accountId}] ${s.ticker}: 匹配`);
            } else {
                console.log(`   ❌ [${s.accountId}] ${s.ticker}: 不匹配`);
                if (s.error) console.log(`      错误: ${s.error}`);
                Object.entries(s.fields).forEach(([f, res]) => {
                    console.log(`      字段 ${f}: FB=${res.fb}, DB=${res.db}`);
                });
            }
        });
        console.log('   -------------------------');
    }

    /**
     * 2. 稽核汇总层: DuckDB Raw -> DuckDB Aggregated
     * @param {number} sampleSize 采样数量
     */
    async auditAggregation(sampleSize = 5) {
        console.log('\n🔍 [Aggregation Audit] 开始汇总层稽核 (Raw -> Aggregated)...');
        const connection = this.createConnection();
        const report = {
            samples: []
        };

        try {
            // 获取所有 aggregated tickers
            const aggrTickers = await this.query(connection, 'SELECT ticker FROM tblHoldingAggrView');
            if (aggrTickers.length === 0) {
                console.log('⚠️ tblHoldingAggrView 为空，跳过稽核');
                return;
            }

            // 随机采样
            const samples = [];
            for (let i = 0; i < sampleSize; i++) {
                const randomIndex = Math.floor(Math.random() * aggrTickers.length);
                samples.push(aggrTickers[randomIndex].ticker);
            }

            // 去重
            const uniqueSamples = [...new Set(samples)];
            console.log(`   正在对 ${uniqueSamples.length} 个标的进行逻辑重算...`);

            // 获取必要的基础数据 (汇率)
            const ratesRows = await this.query(connection, "SELECT fromCurrency, rate FROM tblExchangeRateTTM WHERE toCurrency = 'CNY'");
            const ratesMap = {};
            ratesRows.forEach(r => ratesMap[r.fromCurrency] = r.rate);
            ratesMap['CNY'] = 1.0; // 基础汇率

            for (const ticker of uniqueSamples) {
                const check = {
                    ticker,
                    pass: true,
                    diffs: []
                };

                // 1. 获取 DB 汇总值
                const dbAggrRows = await this.query(connection, 'SELECT * FROM tblHoldingAggrView WHERE ticker = ?', [ticker]);
                if (dbAggrRows.length === 0) {
                    check.pass = false;
                    check.error = 'Missing in AggTable';
                    report.samples.push(check);
                    continue;
                }
                const dbAggr = dbAggrRows[0];

                // 2. 获取 Raw Data 重新计算
                // 需要特别处理 US_TBill 的逻辑，这里先只做普通股票的通用逻辑，如果遇到 US_TBill 特殊处理
                let rawSql = 'SELECT * FROM tblAccountHoldings WHERE ticker = ?';
                let params = [ticker];

                if (ticker === 'US_TBill') {
                    // 重新实现 US Treasury 逻辑
                    rawSql = `
                        SELECT * FROM tblAccountHoldings 
                        WHERE assetClass IN ('BOND', 'Govt') 
                        OR description LIKE '%Treasury%' 
                        OR description LIKE '%T-Bill%'
                        OR ticker = 'US_TBill'
                        OR ticker LIKE 'TF Float%'
                    `;
                    params = [];
                }

                const rawRows = await this.query(connection, rawSql, params);

                // JS 重算逻辑
                let calcTotalHolding = 0;
                let calcTotalCost = 0;
                let calcTotalCostCNY = 0;

                // 获取当前价格 (用于计算 ValueCNY)
                // 注意：这里需要 Quotation 表
                const quoteRows = await this.query(connection, 'SELECT price, currency FROM tblQuotationTTM WHERE ticker = ?', [ticker]);
                const price = (quoteRows.length > 0) ? quoteRows[0].price : 0;
                // US_TBill 特殊价格 1.0 (如果 quoted price 也是 1.0 则一致，否则可能需手动 override)
                const effectivePrice = (ticker === 'US_TBill') ? 1.0 : price;

                rawRows.forEach(row => {
                    calcTotalHolding += row.holding;

                    if (ticker === 'US_TBill') {
                        // US TBill logic: cost is 1.0 * amount
                        calcTotalCost += row.holding * 1.0;
                        // CNY cost
                        const rate = ratesMap[row.currency] || 1.0;
                        calcTotalCostCNY += (row.holding * 1.0) * rate;
                    } else {
                        // Regular logic
                        calcTotalCost += row.holding * row.costPerShare;
                        // CNY cost
                        const rate = ratesMap[row.currency] || 1.0;
                        calcTotalCostCNY += (row.holding * row.costPerShare) * rate;
                    }
                });

                const avgCost = (calcTotalHolding > 0) ? (calcTotalCost / calcTotalHolding) : 0;

                // ValueCNY Calculation
                // 对于 ValueCNY，通常是 totalHolding * currentPrice * Rate
                // 这里汇率稍微复杂，因为 Quote 也有 Currency。
                // 假设 AggLogic 中使用的是 Quote Currency 的汇率
                let quoteCurrency = (quoteRows.length > 0) ? quoteRows[0].currency : 'USD';
                if (ticker === 'US_TBill') quoteCurrency = 'USD'; // force USD

                const quoteRate = ratesMap[quoteCurrency] || 1.0;
                const calcValueCNY = calcTotalHolding * effectivePrice * quoteRate;


                // 3. 比对
                const compare = (field, jVal, dVal) => {
                    // 允许 0.1 的误差 (浮点数)
                    if (Math.abs(jVal - dVal) > 0.1) {
                        check.pass = false;
                        check.diffs.push(`${field}: JS=${jVal.toFixed(2)} vs DB=${dVal.toFixed(2)}`);
                    }
                };

                compare('TotalHolding', calcTotalHolding, dbAggr.totalHolding);
                // compare('TotalCost', calcTotalCost, dbAggr.totalCost); // AggView 可能存的是原币总成本，也可能是聚合后的，视 schema 而定。从代码看是 totalCost (原币混合? 不，aggregationSQL里是 SUM(holding*cost))
                // 如果是多币种混合，totalCost 意义不大，通常看 costCNY
                // 但如果 ticker 维度 aggregation，通常隐含假设同一 ticker 只有一种 currency，或者 AggView 里有 currency 字段代表主货币

                compare('CostCNY', calcTotalCostCNY, dbAggr.costCNY);
                compare('ValueCNY', calcValueCNY, dbAggr.valueCNY);

                report.samples.push(check);
            }

            this.printAggregationReport(report);

        } catch (error) {
            console.error('❌ Aggregation Audit 失败:', error);
        } finally {
            this.closeConnection(connection);
        }
    }

    printAggregationReport(report) {
        console.log('   -------- 稽核报告 --------');
        report.samples.forEach(s => {
            if (s.pass) {
                console.log(`   ✅ ${s.ticker}: 逻辑验证通过`);
            } else {
                console.log(`   ❌ ${s.ticker}: 逻辑验证失败`);
                if (s.error) console.log(`      错误: ${s.error}`);
                s.diffs.forEach(d => console.log(`      ${d}`));
            }
        });
        console.log('   -------------------------');
    }
}

// CLI 入口
async function main() {
    const args = process.argv.slice(2);
    const tool = new AuditTool();
    const sampleSize = 5;

    try {
        if (args.includes('--sync') || args.length === 0) {
            await tool.auditSync(sampleSize);
        }

        if (args.includes('--aggr') || args.length === 0) {
            await tool.auditAggregation(sampleSize);
        }
    } catch (error) {
        console.error('❌ 程序运行出错:', error);
        process.exit(1);
    } finally {
        console.log('\n👋 关闭连接并退出...');
        try {
            await admin.app().delete();
        } catch (e) {
            // 忽略关闭时的错误
        }
        process.exit(0);
    }
}

if (require.main === module) {
    main();
}

// toolUpdateLocalAssetPool.js
const admin = require('firebase-admin');
const duckdb = require('duckdb');
const path = require('path');
const fs = require('fs');

// 配置路径
const SERVICE_KEY_PATH = '/Users/zhangqing/Documents/Github/serviceKeys/bramblingV2Firebase.json';
const DUCK_DB_PATH = path.join(__dirname, '../duckDB/dealLogs.duckdb');

// 初始化 Firebase
if (!fs.existsSync(SERVICE_KEY_PATH)) {
    console.error(`❌ 找不到 Service Account Key: ${SERVICE_KEY_PATH}`);
    process.exit(1);
}

const serviceAccount = require(SERVICE_KEY_PATH);

// 防止重复初始化
if (!admin.apps.length) {
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        databaseURL: 'https://outpost-8d74e-14018.firebaseio.com/'
    });
}
const db = admin.database();

// 初始化 DuckDB
const duckDB = new duckdb.Database(DUCK_DB_PATH, (err) => {
    if (err) {
        console.error('❌ Failed to open DuckDB database:', err);
        process.exit(1);
    }
});
const connection = duckDB.connect();

function runCommand(query, params = []) {
    return new Promise((resolve, reject) => {
        connection.run(query, ...params, (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

async function syncAssetPool() {
    console.log('🚀 开始从 Firebase 同步 Asset Pool 到本地 DuckDB...');

    try {
        // 1. 获取 Firebase 数据
        const snapshot = await db.ref('assetPool').once('value');
        const assets = snapshot.val();

        if (!assets) {
            console.log('⚠️ Firebase 中没有 assetPool 数据');
            return;
        }

        const entries = Object.values(assets);
        console.log(`📡 获取到 ${entries.length} 条资产记录`);

        // 2. 准备 DuckDB 表
        await runCommand(`
            CREATE TABLE IF NOT EXISTS assetPoolTbl (
                ticker VARCHAR PRIMARY KEY,
                company VARCHAR,
                currency VARCHAR,
                exchange VARCHAR,
                country VARCHAR,
                status VARCHAR,
                last_synced TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // 3. 批量插入/更新
        // 由于 DuckDB 的 INSERT OR REPLACE 语法可能因版本而异，这里使用 DELETE + INSERT 的简单策略，或者 ON CONFLICT (如果支持)
        // 为确保兼容性，先清空表或者使用 INSERT OR IGNORE (如果只增不减)，这里采用全量覆盖策略最简单，因为是本地缓存
        // 但为了保留潜在的本地修改（如果有），我们使用 INSERT OR REPLACE INTO 

        await runCommand('BEGIN TRANSACTION');

        let count = 0;
        for (const asset of entries) {
            await runCommand(`
                INSERT OR REPLACE INTO assetPoolTbl (ticker, company, currency, exchange, country, status, last_synced)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            `, [
                asset.ticker,
                asset.company || '',
                asset.currency || '',
                asset.exchange || '',
                asset.country || '',
                asset.status || 'watching'
            ]);
            count++;
        }

        await runCommand('COMMIT');
        console.log(`✅ 成功同步 ${count} 条记录到 assetPoolTbl`);

    } catch (error) {
        console.error('❌ 同步失败:', error);
        try { await runCommand('ROLLBACK'); } catch (e) { }
    } finally {
        // 关闭连接
        connection.close();
        process.exit(0);
    }
}

// 运行
if (require.main === module) {
    syncAssetPool();
}

module.exports = syncAssetPool;

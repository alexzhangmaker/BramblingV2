const { createClient } = require('@supabase/supabase-js');
const duckdb = require('duckdb');
const async = require('async');

class NodeDataSync {
    constructor() {
        // 初始化 Supabase 客户端
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
        
        // 初始化 DuckDB（使用文件持久化或内存）
        this.db = new duckdb.Database(':memory:'); // 或 ':memory:' 用于测试
        this.connection = new duckdb.Connection(this.db);
        
        this.setupTables();
    }

    async setupTables() {
        // 创建交易数据表
        await this.connection.run(`
            CREATE TABLE IF NOT EXISTS transactions (
                id VARCHAR PRIMARY KEY,
                amount DOUBLE,
                user_id VARCHAR,
                category VARCHAR,
                created_at TIMESTAMP
            )
        `);
    }

    async syncToDuckDB(hours = 24) {
        try {
            const cutoffTime = new Date();
            cutoffTime.setHours(cutoffTime.getHours() - hours);

            // 从 Supabase 获取数据
            const { data, error } = await this.supabase
                .from('transactions')
                .select('*')
                .gte('created_at', cutoffTime.toISOString());

            if (error) throw error;

            if (data && data.length > 0) {
                // 批量插入到 DuckDB
                const placeholders = data.map((_, index) => 
                    `($${index * 5 + 1}, $${index * 5 + 2}, $${index * 5 + 3}, $${index * 5 + 4}, $${index * 5 + 5})`
                ).join(',');

                const values = data.flatMap(row => [
                    row.id, row.amount, row.user_id, row.category, row.created_at
                ]);

                await this.connection.run(`
                    INSERT OR REPLACE INTO transactions 
                    VALUES ${placeholders}
                `, values);

                console.log(`✅ 同步 ${data.length} 条交易记录到 DuckDB`);
            } else {
                console.log('ℹ️ 没有新交易数据');
            }

        } catch (error) {
            console.error('❌ 同步失败:', error);
        }
    }

    async runAnalytics() {
        try {
            // 在 DuckDB 中执行分析查询
            const result = await new Promise((resolve, reject) => {
                this.connection.all(`
                    SELECT 
                        user_id,
                        DATE(created_at) as date,
                        COUNT(*) as transaction_count,
                        SUM(amount) as total_amount,
                        AVG(amount) as avg_amount
                    FROM transactions 
                    GROUP BY user_id, DATE(created_at)
                    ORDER BY date DESC, total_amount DESC
                `, (err, res) => {
                    if (err) reject(err);
                    else resolve(res);
                });
            });

            if (result.length > 0) {
                // 同步分析结果回 Supabase
                const { error } = await this.supabase
                    .from('analytics_results')
                    .upsert(result, { onConflict: 'user_id,date' });

                if (error) throw error;

                console.log(`✅ 同步 ${result.length} 条分析结果到 Supabase`);
            }

            return result;

        } catch (error) {
            console.error('❌ 分析执行失败:', error);
            throw error;
        }
    }

    async startSyncService() {
        console.log('🔄 启动数据同步服务...');
        
        // 立即执行一次同步
        await this.syncToDuckDB();
        await this.runAnalytics();
        
        // 每5分钟同步一次
        setInterval(async () => {
            await this.syncToDuckDB();
            await this.runAnalytics();
        }, 5 * 60 * 1000);
    }
}

// 使用示例
const syncService = new NodeDataSync();
syncService.startSyncService();

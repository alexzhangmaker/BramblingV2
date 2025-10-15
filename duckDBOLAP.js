class OLAPAnalytics {
    constructor() {
        this.db = new duckdb.Database(':memory:');
        this.conn = new duckdb.Connection(this.db);
    }

    async createMaterializedViews() {
        // 创建物化视图（DuckDB 支持）
        await this.conn.run(`
            CREATE MATERIALIZED VIEW IF NOT EXISTS daily_sales_mv AS
            SELECT 
                user_id,
                DATE(created_at) as date,
                COUNT(*) as transaction_count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount,
                -- 窗口函数计算移动平均
                AVG(amount) OVER (
                    PARTITION BY user_id 
                    ORDER BY DATE(created_at) 
                    ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
                ) as moving_avg_7d
            FROM transactions 
            GROUP BY user_id, DATE(created_at)
        `);

        // 创建多维分析物化视图
        await this.conn.run(`
            CREATE MATERIALIZED VIEW IF NOT EXISTS sales_cube_mv AS
            SELECT 
                user_id,
                category,
                DATE_TRUNC('week', created_at) as week,
                DATE_TRUNC('month', created_at) as month,
                COUNT(*) as count,
                SUM(amount) as total,
                -- 百分比计算
                SUM(amount) * 100.0 / SUM(SUM(amount)) OVER () as pct_total
            FROM transactions 
            GROUP BY 
                GROUPING SETS (
                    (user_id, category, week),
                    (user_id, month),
                    (category),
                    ()
                )
        `);
    }

    async refreshMaterializedViews() {
        // 刷新物化视图
        await this.conn.run('REFRESH MATERIALIZED VIEW daily_sales_mv');
        await this.conn.run('REFRESH MATERIALIZED VIEW sales_cube_mv');
    }
}


class AggregationEngine {
    constructor() {
        this.conn = new duckdb.Connection(new duckdb.Database(':memory:'));
        this.aggregationTables = new Map();
    }

    // 定义聚合规则
    defineAggregation(name, config) {
        this.aggregationTables.set(name, config);
    }

    // 执行聚合计算
    async computeAggregation(name, refresh = false) {
        const config = this.aggregationTables.get(name);
        if (!config) throw new Error(`聚合 ${name} 未定义`);

        const tableName = `agg_${name}`;

        if (refresh) {
            await this.conn.run(`DROP TABLE IF EXISTS ${tableName}`);
        }

        const query = `
            CREATE TABLE IF NOT EXISTS ${tableName} AS
            ${config.sql}
        `;

        await this.conn.run(query);
        console.log(`✅ 聚合表 ${tableName} 计算完成`);
    }

    // 查询聚合结果
    async queryAggregation(name, dimensions = [], measures = []) {
        const tableName = `agg_${name}`;
        
        const dims = dimensions.length > 0 ? dimensions.join(', ') : '*';
        const where = measures.length > 0 
            ? `WHERE measure_name IN (${measures.map(m => `'${m}'`).join(', ')})`
            : '';

        const query = `
            SELECT ${dims} FROM ${tableName} ${where}
        `;

        return new Promise((resolve, reject) => {
            this.conn.all(query, (err, res) => {
                if (err) reject(err);
                else resolve(res);
            });
        });
    }
}

/*
// 使用示例
const engine = new AggregationEngine();

// 定义日聚合
engine.defineAggregation('daily_sales', {
    sql: `
        SELECT 
            user_id,
            DATE(created_at) as date,
            'total_sales' as measure_name,
            SUM(amount) as measure_value
        FROM transactions 
        GROUP BY user_id, DATE(created_at)
        
        UNION ALL
        
        SELECT 
            user_id,
            DATE(created_at) as date,
            'avg_sales' as measure_name,
            AVG(amount) as measure_value
        FROM transactions 
        GROUP BY user_id, DATE(created_at)
    `
});

// 定义周聚合
engine.defineAggregation('weekly_metrics', {
    sql: `
        SELECT 
            user_id,
            DATE_TRUNC('week', created_at) as week_start,
            category,
            COUNT(*) as transaction_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount,
            MAX(amount) as max_amount,
            MIN(amount) as min_amount
        FROM transactions 
        GROUP BY user_id, DATE_TRUNC('week', created_at), category
    `
});
*/


class TimeSeriesAnalytics {
    constructor() {
        this.conn = new duckdb.Connection(new duckdb.Database(':memory:'));
    }

    // 时间序列聚合
    async timeSeriesAggregation(table, timeColumn, valueColumn, interval = 'day') {
        const query = `
            SELECT
                DATE_TRUNC('${interval}', ${timeColumn}) as time_bucket,
                COUNT(*) as count,
                SUM(${valueColumn}) as total,
                AVG(${valueColumn}) as average,
                STDDEV(${valueColumn}) as std_dev,
                -- 同比计算
                LAG(total, 7) OVER (ORDER BY time_bucket) as previous_period,
                (total - LAG(total, 7) OVER (ORDER BY time_bucket)) * 100.0 / 
                LAG(total, 7) OVER (ORDER BY time_bucket) as growth_rate
            FROM ${table}
            GROUP BY time_bucket
            ORDER BY time_bucket
        `;

        return new Promise((resolve, reject) => {
            this.conn.all(query, (err, res) => {
                if (err) reject(err);
                else resolve(res);
            });
        });
    }

    // 移动平均计算
    async movingAverage(table, valueColumn, windowSize = 7) {
        const query = `
            SELECT
                date,
                value,
                AVG(value) OVER (
                    ORDER BY date 
                    ROWS BETWEEN ${windowSize - 1} PRECEDING AND CURRENT ROW
                ) as moving_avg
            FROM ${table}
        `;

        return new Promise((resolve, reject) => {
            this.conn.all(query, (err, res) => {
                if (err) reject(err);
                else resolve(res);
            });
        });
    }
}

class BusinessAnalytics {
    constructor() {
        this.engine = new AggregationEngine();
        this.setupAggregations();
    }

    setupAggregations() {
        // 用户行为分析
        this.engine.defineAggregation('user_behavior', {
            sql: `
                SELECT 
                    user_id,
                    DATE(created_at) as date,
                    COUNT(*) as daily_transactions,
                    SUM(amount) as daily_spend,
                    AVG(amount) as avg_transaction_size,
                    COUNT(DISTINCT category) as unique_categories
                FROM transactions 
                GROUP BY user_id, DATE(created_at)
            `
        });

        // 品类分析
        this.engine.defineAggregation('category_analysis', {
            sql: `
                SELECT 
                    category,
                    DATE_TRUNC('week', created_at) as week,
                    COUNT(*) as transaction_count,
                    SUM(amount) as total_revenue,
                    COUNT(DISTINCT user_id) as unique_customers,
                    total_revenue / unique_customers as revenue_per_customer
                FROM transactions 
                GROUP BY category, DATE_TRUNC('week', created_at)
            `
        });
    }

    async refreshAllAggregations() {
        const aggregations = ['user_behavior', 'category_analysis'];
        
        for (const agg of aggregations) {
            await this.engine.computeAggregation(agg, true);
        }
        
        console.log('✅ 所有聚合表刷新完成');
    }

    async getDashboardData() {
        const [userStats, categoryStats] = await Promise.all([
            this.engine.queryAggregation('user_behavior'),
            this.engine.queryAggregation('category_analysis')
        ]);

        return {
            userStats,
            categoryStats,
            lastUpdated: new Date().toISOString()
        };
    }
}
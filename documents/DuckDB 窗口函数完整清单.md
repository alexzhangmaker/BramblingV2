对的！窗口函数是 OLAP 的核心功能之一。我来给你一个完整的 DuckDB 窗口函数功能清单：

## DuckDB 窗口函数完整清单

### 1. 排名函数 (Ranking Functions)

```sql
-- 数据准备
CREATE TABLE sales (
    salesperson VARCHAR,
    region VARCHAR,
    amount DECIMAL,
    sale_date DATE
);

-- 1. ROW_NUMBER() - 行号
SELECT 
    salesperson,
    amount,
    ROW_NUMBER() OVER (ORDER BY amount DESC) as rank
FROM sales;

-- 2. RANK() - 排名（允许并列）
SELECT 
    salesperson,
    amount,
    RANK() OVER (ORDER BY amount DESC) as rank
FROM sales;

-- 3. DENSE_RANK() - 密集排名（无间隔）
SELECT 
    salesperson,
    amount,
    DENSE_RANK() OVER (ORDER BY amount DESC) as dense_rank
FROM sales;

-- 4. PERCENT_RANK() - 百分比排名
SELECT 
    salesperson,
    amount,
    PERCENT_RANK() OVER (ORDER BY amount) as pct_rank
FROM sales;

-- 5. NTILE(n) - 分桶
SELECT 
    salesperson,
    amount,
    NTILE(4) OVER (ORDER BY amount DESC) as quartile
FROM sales;
```

### 2. 分布函数 (Distribution Functions)

```sql
-- 6. CUME_DIST() - 累积分布
SELECT 
    salesperson,
    amount,
    CUME_DIST() OVER (ORDER BY amount) as cumulative_dist
FROM sales;

-- 7. PERCENTILE_CONT() - 连续百分位数
SELECT 
    salesperson,
    amount,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) 
        OVER (PARTITION BY region) as median
FROM sales;

-- 8. PERCENTILE_DISC() - 离散百分位数
SELECT 
    salesperson,
    amount,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY amount) 
        OVER (PARTITION BY region) as median_discrete
FROM sales;
```

### 3. 偏移函数 (Offset Functions)

```sql
-- 9. LAG() - 前一行
SELECT 
    salesperson,
    sale_date,
    amount,
    LAG(amount, 1) OVER (
        PARTITION BY salesperson 
        ORDER BY sale_date
    ) as previous_amount
FROM sales;

-- 10. LEAD() - 后一行
SELECT 
    salesperson,
    sale_date,
    amount,
    LEAD(amount, 1) OVER (
        PARTITION BY salesperson 
        ORDER BY sale_date
    ) as next_amount
FROM sales;

-- 11. FIRST_VALUE() - 窗口第一个值
SELECT 
    salesperson,
    sale_date,
    amount,
    FIRST_VALUE(amount) OVER (
        PARTITION BY salesperson 
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as first_amount
FROM sales;

-- 12. LAST_VALUE() - 窗口最后一个值
SELECT 
    salesperson,
    sale_date,
    amount,
    LAST_VALUE(amount) OVER (
        PARTITION BY salesperson 
        ORDER BY sale_date
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) as last_amount
FROM sales;

-- 13. NTH_VALUE() - 第N个值
SELECT 
    salesperson,
    sale_date,
    amount,
    NTH_VALUE(amount, 2) OVER (
        PARTITION BY salesperson 
        ORDER BY sale_date
    ) as second_amount
FROM sales;
```

### 4. 聚合函数 + 窗口 (Aggregate Functions with Window)

```sql
-- 14. SUM() OVER - 累计求和
SELECT 
    salesperson,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY salesperson 
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_total
FROM sales;

-- 15. AVG() OVER - 移动平均
SELECT 
    salesperson,
    sale_date,
    amount,
    AVG(amount) OVER (
        PARTITION BY salesperson 
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7d
FROM sales;

-- 16. COUNT() OVER - 累计计数
SELECT 
    salesperson,
    sale_date,
    amount,
    COUNT(*) OVER (
        PARTITION BY salesperson 
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as transaction_count
FROM sales;

-- 17. MIN/MAX OVER - 窗口最小最大值
SELECT 
    salesperson,
    sale_date,
    amount,
    MIN(amount) OVER (
        PARTITION BY salesperson 
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as min_so_far,
    MAX(amount) OVER (
        PARTITION BY salesperson 
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as max_so_far
FROM sales;
```

### 5. 统计函数 (Statistical Functions)

```sql
-- 18. STDDEV() / STDDEV_POP() / STDDEV_SAMP()
SELECT 
    salesperson,
    region,
    amount,
    STDDEV(amount) OVER (PARTITION BY region) as std_dev_amount
FROM sales;

-- 19. VARIANCE() / VAR_POP() / VAR_SAMP()
SELECT 
    salesperson,
    region,
    amount,
    VARIANCE(amount) OVER (PARTITION BY region) as variance_amount
FROM sales;

-- 20. COVAR_POP() / COVAR_SAMP() - 协方差
SELECT 
    salesperson,
    amount,
    another_metric,
    COVAR_POP(amount, another_metric) OVER () as covariance
FROM sales;

-- 21. CORR() - 相关系数
SELECT 
    salesperson,
    amount,
    another_metric,
    CORR(amount, another_metric) OVER () as correlation
FROM sales;
```

## 窗口框架详解

### 窗口范围定义

```sql
-- ROWS 模式 - 物理行
ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING        -- 前后2行
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW -- 从开始到当前
ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING -- 从当前到最后

-- RANGE 模式 - 逻辑值范围
RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW
RANGE BETWEEN 100 PRECEDING AND 200 FOLLOWING

-- GROUPS 模式 - 分组范围
GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING
```

## 实际业务场景示例

```sql
-- 场景1: 销售排名分析
SELECT 
    salesperson,
    region,
    amount,
    -- 总体排名
    RANK() OVER (ORDER BY amount DESC) as overall_rank,
    -- 区域内排名
    RANK() OVER (PARTITION BY region ORDER BY amount DESC) as region_rank,
    -- 与区域第一名的差距
    FIRST_VALUE(amount) OVER (PARTITION BY region ORDER BY amount DESC) - amount as gap_to_leader
FROM sales;

-- 场景2: 时间序列分析
SELECT 
    user_id,
    transaction_date,
    amount,
    -- 7天移动平均
    AVG(amount) OVER (
        PARTITION BY user_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL '6' DAY PRECEDING AND CURRENT ROW
    ) as moving_avg_7d,
    -- 环比增长
    amount - LAG(amount, 1) OVER (
        PARTITION BY user_id 
        ORDER BY transaction_date
    ) as day_over_day_growth
FROM transactions;

-- 场景3: 用户行为分析
SELECT 
    user_id,
    session_start,
    page_views,
    -- 累计页面浏览
    SUM(page_views) OVER (
        PARTITION BY user_id 
        ORDER BY session_start 
        ROWS UNBOUNDED PRECEDING
    ) as cumulative_views,
    -- 最近3次会话平均
    AVG(page_views) OVER (
        PARTITION BY user_id 
        ORDER BY session_start 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as avg_last_3_sessions
FROM user_sessions;
```

## Node.js 中使用示例

```javascript
class WindowFunctionExamples {
    async runSalesRanking() {
        const query = `
            SELECT 
                salesperson,
                region,
                amount,
                RANK() OVER (ORDER BY amount DESC) as overall_rank,
                RANK() OVER (PARTITION BY region ORDER BY amount DESC) as region_rank,
                amount - FIRST_VALUE(amount) OVER (
                    PARTITION BY region ORDER BY amount DESC
                ) as gap_to_region_leader
            FROM sales
            ORDER BY overall_rank
        `;
        
        return new Promise((resolve, reject) => {
            this.conn.all(query, (err, res) => {
                if (err) reject(err);
                else resolve(res);
            });
        });
    }
    
    async runTimeSeriesAnalysis() {
        const query = `
            SELECT 
                user_id,
                transaction_date,
                amount,
                AVG(amount) OVER (
                    PARTITION BY user_id 
                    ORDER BY transaction_date 
                    RANGE BETWEEN INTERVAL '6' DAY PRECEDING AND CURRENT ROW
                ) as moving_avg_7d,
                (amount - LAG(amount, 1) OVER (
                    PARTITION BY user_id 
                    ORDER BY transaction_date
                )) * 100.0 / LAG(amount, 1) OVER (
                    PARTITION BY user_id 
                    ORDER BY transaction_date
                ) as growth_rate_pct
            FROM transactions
            ORDER BY user_id, transaction_date
        `;
        
        return new Promise((resolve, reject) => {
            this.conn.all(query, (err, res) => {
                if (err) reject(err);
                else resolve(res);
            });
        });
    }
}
```

这个清单涵盖了 DuckDB 中绝大部分窗口函数，可以满足各种复杂的 OLAP 分析需求！
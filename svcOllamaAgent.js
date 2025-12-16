// svcOllamaAgent.js
const express = require('express');
const bodyParser = require('body-parser');
const duckdb = require('duckdb');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = 3000;
const OLLAMA_URL = 'http://localhost:11434/api/generate';
const MODEL_NAME = 'llama3';

// 数据库配置
const DUCKDB_DIR = path.join(__dirname, 'duckDB');
const DB_PATH = path.join(DUCKDB_DIR, 'dealLogs.duckdb');

// 确保目录存在
if (!fs.existsSync(DUCKDB_DIR)) {
    fs.mkdirSync(DUCKDB_DIR, { recursive: true });
}

// 初始化数据库连接
const db = new duckdb.Database(DB_PATH);
const connection = db.connect();

// 初始化表结构
connection.run(`
  CREATE TABLE IF NOT EXISTS dealLogsTbl (
    id INTEGER PRIMARY KEY,
    action VARCHAR,
    ticker VARCHAR,
    amount DOUBLE,
    price DOUBLE,
    currency VARCHAR,
    total DOUBLE,
    date DATE,
    rawText VARCHAR,
    status VARCHAR DEFAULT 'toAudit',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  CREATE SEQUENCE IF NOT EXISTS seq_deal_logs_id START 1;
`);

// 缓存的 Ticker 列表
let cachedTickers = [];

/**
 * 加载 Asset Pool 中的 Tickers 和 Company Names
 */
async function loadAssetTickers() {
    try {
        const result = await runQuery('SELECT ticker, company FROM assetPoolTbl');
        if (result && result.length > 0) {
            // 格式化为 "TICKER (Company Name)"，方便 LLM 关联
            cachedTickers = result.map(row => {
                const company = row.company ? ` (${row.company})` : '';
                return `${row.ticker}${company}`;
            });
            console.log(`📚 已加载 ${cachedTickers.length} 个已知资产 (Ticker + Company) 供 Ollama 参考`);
        }
    } catch (error) {
        console.warn('⚠️ 加载 Asset Tickers 失败 (可能表还未创建):', error.message);
    }
}

// 启动时尝试加载一次
setTimeout(loadAssetTickers, 2000);

app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, 'public'))); // 服务静态文件

// ================= 数据库工具函数 =================

function runQuery(query, params = []) {
    return new Promise((resolve, reject) => {
        connection.all(query, ...params, (err, result) => {
            if (err) reject(err);
            else resolve(result);
        });
    });
}

function runCommand(query, params = []) {
    return new Promise((resolve, reject) => {
        connection.run(query, ...params, (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

// ================= Ollama 相关函数 =================

async function queryOllama(prompt, systemPrompt = '', model = MODEL_NAME) {
    try {
        const response = await fetch(OLLAMA_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                model: model,
                prompt: prompt,
                system: systemPrompt,
                stream: false,
                format: 'json'
            }),
        });

        if (!response.ok) throw new Error(`Ollama API error: ${response.statusText}`);
        const data = await response.json();
        return data.response;
    } catch (error) {
        console.error('❌ Ollama 请求失败:', error.message);
        throw error;
    }
}

async function handleLogDeal(text) {
    const systemPrompt = `
    You are a financial data assistant. Your job is to extract transaction details from natural language text and return them as a STRICT JSON object.
    The JSON object should have: action (BUY/SELL/...), ticker (uppercase), amount (number), price (number), currency (USD/HKD/...), total (number), date (YYYY-MM-DD or null).
    If any field is missing, use null (except date which can be null).
    
    IMPORTANT: Here is a list of known valid assets in format "TICKER (Company Name)". 
    1. If the user mentions a company name (Chinese or English) that matches one of these, YOU MUST RETURN THE CORRESPONDING TICKER.
    2. If the user input contains typos or partial names (e.g., "长电电力" instead of "长江电力"), please infer the most likely match from the list and use that Ticker.
    
    Specific Disambiguation Rules:
    - If user says "Brookfield" without further qualification, default to "BN" (Brookfield Corp), NOT "BF-B" (Brown-Forman) or "BAM".
    - "BF-B" is ONLY for "Brown-Forman".
    
    Known Assets:
    [${cachedTickers.join(', ')}]
  `;
    const userPrompt = `Parse this transaction log: "${text}"`;
    const resultString = await queryOllama(userPrompt, systemPrompt);
    try {
        return JSON.parse(resultString);
    } catch (e) {
        console.warn('JSON Parse Error, trying repair...');
        // 简单的容错处理或直接抛出
        throw new Error('Failed to parse Ollama response as JSON');
    }
}

// ================= API 路由 =================

/**
 * 1. 提交新交易日志 (Parse + Save)
 * POST /api/log-deal
 */
app.post('/api/log-deal', async (req, res) => {
    const { text } = req.body;
    if (!text) return res.status(400).json({ error: 'Missing "text"' });

    console.log(`📩 收到 LogDeal: "${text}"`);
    try {
        const data = await handleLogDeal(text);

        // 存入数据库
        // DuckDB 的 SEQUENCE 使用 nextval('seq_name')
        await runCommand(`
      INSERT INTO dealLogsTbl (id, action, ticker, amount, price, currency, total, date, rawText, status)
      VALUES (nextval('seq_deal_logs_id'), ?, ?, ?, ?, ?, ?, ?, ?, 'toAudit')
    `, [
            data.action || null,
            data.ticker || null,
            data.amount || 0,
            data.price || 0,
            data.currency || 'USD',
            data.total || ((data.amount || 0) * (data.price || 0)),
            data.date || new Date().toISOString().split('T')[0], // Default to today if date is missing
            text
        ]);

        console.log('✅ 解析并保存成功:', data);
        res.json({ success: true, data, message: 'Saved to dealLogsTbl with status "toAudit"' });

    } catch (error) {
        console.error(error);
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * 2. 查询交易日志 (List)
 * GET /api/deals
 * 可选参数: status (toAudit, audited, etc.)
 */
app.get('/api/deals', async (req, res) => {
    try {
        let query = 'SELECT * FROM dealLogsTbl';
        const params = [];

        if (req.query.status) {
            query += ' WHERE status = ?';
            params.push(req.query.status);
        }

        query += ' ORDER BY created_at DESC';

        const logs = await runQuery(query, params);
        res.json({ success: true, data: logs });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * 3. 修改交易日志 (Update)
 * PUT /api/deals/:id
 */
app.put('/api/deals/:id', async (req, res) => {
    const { id } = req.params;
    const updates = req.body; // Expect keys like action, ticker, price, etc.

    if (Object.keys(updates).length === 0) {
        return res.status(400).json({ error: 'No update fields provided' });
    }

    try {
        const fields = [];
        const values = [];

        for (const [key, value] of Object.entries(updates)) {
            if (['action', 'ticker', 'amount', 'price', 'currency', 'total', 'date', 'status', 'rawText'].includes(key)) {
                fields.push(`${key} = ?`);
                values.push(value);
            }
        }

        if (fields.length === 0) return res.status(400).json({ error: 'Invalid fields' });

        values.push(id);
        await runCommand(`UPDATE dealLogsTbl SET ${fields.join(', ')} WHERE id = ?`, values);

        res.json({ success: true, message: `Deal ${id} updated` });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * 4. 删除交易日志 (Delete)
 * DELETE /api/deals/:id
 */
app.delete('/api/deals/:id', async (req, res) => {
    const { id } = req.params;
    try {
        await runCommand('DELETE FROM dealLogsTbl WHERE id = ?', [id]);
        res.json({ success: true, message: `Deal ${id} deleted` });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * 5. 审计操作 (Audit)
 * POST /api/deals/:id/audit
 * Body: { "status": "audited" }  (默认为 'audited')
 */
app.post('/api/deals/:id/audit', async (req, res) => {
    const { id } = req.params;
    const status = req.body.status || 'audited'; // 目标状态

    try {
        await runCommand('UPDATE dealLogsTbl SET status = ? WHERE id = ?', [status, id]);
        res.json({ success: true, message: `Deal ${id} status changed to ${status}` });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ================= 启动服务 =================

app.listen(PORT, () => {
    console.log(`🚀 svcOllamaAgent (DuckDB Enhanced) running on port ${PORT}`);
    console.log(`💾 Database: ${DB_PATH}`);
});

module.exports = app;

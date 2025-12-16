const duckdb = require('duckdb');
const path = require('path');

const dbPath = path.join(__dirname, 'duckDB/dealLogs.duckdb');
const db = new duckdb.Database(dbPath);

db.all("SELECT ticker, company FROM assetPoolTbl WHERE company LIKE '%Brookfield%' OR ticker IN ('BN', 'BF-B', 'BF.B', 'BAM')", (err, res) => {
    if (err) console.error(err);
    else console.log(JSON.stringify(res, null, 2));
});

// toolDuckDB2Firebase.js
const duckdb = require('duckdb');
const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');

// 初始化Firebase
const serviceAccount = require('/Users/zhangqing/Documents/Github/serviceKeys/bramblingV2Firebase.json');
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: 'https://outpost-8d74e-14018.firebaseio.com/'
});

const db = admin.database();
const duckDbFilePath = path.join(__dirname, '../duckDB/PortfolioData.duckdb');

class DuckDBToFirebaseExporter {
  constructor() {
    this.dbInstance = new duckdb.Database(duckDbFilePath);
  }

  createConnection() {
    const connection = this.dbInstance.connect();
    connection.run("PRAGMA threads=4");
    connection.run("PRAGMA memory_limit='1GB'");
    return connection;
  }

  closeConnection(connection) {
    if (connection) {
      try {
        connection.close();
      } catch (error) {
        console.warn('关闭连接时出现警告:', error.message);
      }
    }
  }

  async safeQuery(connection, query, params = []) {
    return new Promise((resolve, reject) => {
      if (params.length === 0) {
        connection.all(query, (err, result) => {
          if (err) reject(err);
          else resolve(Array.isArray(result) ? result : []);
        });
      } else {
        connection.all(query, ...params, (err, result) => {
          if (err) reject(err);
          else resolve(Array.isArray(result) ? result : []);
        });
      }
    });
  }

  /**
   * 编码 Firebase key（替换不允许的字符）
   */
  encodeFirebaseKey(key) {
    if (typeof key !== 'string') {
      key = String(key);
    }

    // 替换 Firebase 不允许的字符
    return key
      .replace(/\./g, '_DOT_')
      .replace(/\#/g, '_HASH_')
      .replace(/\$/g, '_DOLLAR_')
      .replace(/\//g, '_SLASH_')
      .replace(/\[/g, '_LBRACKET_')
      .replace(/\]/g, '_RBRACKET_')
      .replace(/\s+/g, '_SPACE_');
  }

  /**
   * 解码 Firebase key（恢复原始 key）
   */
  decodeFirebaseKey(encodedKey) {
    return encodedKey
      .replace(/_DOT_/g, '.')
      .replace(/_HASH_/g, '#')
      .replace(/_DOLLAR_/g, '$')
      .replace(/_SLASH_/g, '/')
      .replace(/_LBRACKET_/g, '[')
      .replace(/_RBRACKET_/g, ']')
      .replace(/_SPACE_/g, ' ');
  }

  /**
   * 获取表结构信息
   */
  async getTableStructure(connection, tableName) {
    try {
      const structure = await this.safeQuery(connection, `PRAGMA table_info(${tableName})`);
      return structure;
    } catch (error) {
      console.error(`❌ 获取表 ${tableName} 结构失败:`, error.message);
      return [];
    }
  }

  /**
   * 获取表数据
   */
  async getTableData(connection, tableName) {
    try {
      const data = await this.safeQuery(connection, `SELECT * FROM ${tableName}`);
      return data;
    } catch (error) {
      console.error(`❌ 获取表 ${tableName} 数据失败:`, error.message);
      return [];
    }
  }

  /**
   * 转换数据格式（处理特殊类型）
   */
  convertDataForFirebase(data) {
    return data.map(row => {
      const convertedRow = {};
      for (const [key, value] of Object.entries(row)) {
        // 处理 Date 对象
        if (value instanceof Date) {
          convertedRow[key] = value.toISOString();
        }
        // 处理 Buffer 或其他特殊类型
        else if (value && typeof value === 'object' && !Array.isArray(value)) {
          convertedRow[key] = JSON.stringify(value);
        }
        // 处理 NaN 和 Infinity
        else if (typeof value === 'number' && !isFinite(value)) {
          convertedRow[key] = null;
        }
        // 保持其他类型不变
        else {
          convertedRow[key] = value;
        }
      }
      return convertedRow;
    });
  }

  /**
   * 将数据写入 Firebase
   */
  async writeToFirebase(data, firebasePath, options = {}) {
    const {
      batchSize = 100,
      primaryKey = null,
      overwrite = false,
      encodeKeys = true
    } = options;

    try {
      console.log(`📤 开始写入数据到 Firebase 路径: ${firebasePath}`);

      const firebaseRef = db.ref(firebasePath);

      if (overwrite) {
        console.log('🗑️  清空现有数据...');
        await firebaseRef.remove();
        console.log('✅ 现有数据已清空');
      }

      let successCount = 0;
      let errorCount = 0;

      // 如果有主键，按主键分批写入
      if (primaryKey) {
        console.log(`🔑 使用主键: ${primaryKey}`);
        if (encodeKeys) {
          console.log('🔤 启用主键编码（自动处理特殊字符）');
        }

        for (let i = 0; i < data.length; i += batchSize) {
          const batch = data.slice(i, i + batchSize);
          const updates = {};

          batch.forEach(item => {
            let key = item[primaryKey];
            if (key) {
              // 编码主键（如果需要）
              if (encodeKeys) {
                key = this.encodeFirebaseKey(key);
              }
              updates[key] = item;
            } else {
              console.warn(`⚠️ 记录缺少主键 ${primaryKey}:`, item);
            }
          });

          try {
            await firebaseRef.update(updates);
            successCount += Object.keys(updates).length;
            console.log(`✅ 批次 ${Math.floor(i / batchSize) + 1} 写入完成: ${Object.keys(updates).length} 条记录`);

            // 显示一些编码示例（第一个批次）
            if (i === 0 && encodeKeys && Object.keys(updates).length > 0) {
              console.log('🔤 主键编码示例:');
              const sampleKeys = Object.keys(updates).slice(0, 3);
              sampleKeys.forEach(encodedKey => {
                const originalKey = this.decodeFirebaseKey(encodedKey);
                console.log(`   ${originalKey} → ${encodedKey}`);
              });
            }
          } catch (error) {
            errorCount += batch.length;
            console.error(`❌ 批次 ${Math.floor(i / batchSize) + 1} 写入失败:`, error.message);

            // 显示有问题的 key（第一个失败批次）
            if (errorCount === batch.length) {
              console.log('🔍 有问题的 key 示例:');
              const problematicKeys = batch.slice(0, 3).map(item => item[primaryKey]);
              problematicKeys.forEach(key => {
                console.log(`   ${key} → ${this.encodeFirebaseKey(key)}`);
              });
            }
          }
        }
      } else {
        // 没有主键，直接写入数组
        console.log('📝 无主键模式，写入数组数据...');

        for (let i = 0; i < data.length; i += batchSize) {
          const batch = data.slice(i, i + batchSize);

          try {
            // 使用 push() 方法添加记录，Firebase 会自动生成 key
            const promises = batch.map(item => firebaseRef.push(item));
            await Promise.all(promises);

            successCount += batch.length;
            console.log(`✅ 批次 ${Math.floor(i / batchSize) + 1} 写入完成: ${batch.length} 条记录`);
          } catch (error) {
            errorCount += batch.length;
            console.error(`❌ 批次 ${Math.floor(i / batchSize) + 1} 写入失败:`, error.message);
          }
        }
      }

      console.log(`\n📊 写入完成统计:`);
      console.log(`   成功: ${successCount} 条记录`);
      console.log(`   失败: ${errorCount} 条记录`);
      console.log(`   总计: ${data.length} 条记录`);

      return { successCount, errorCount, total: data.length };

    } catch (error) {
      console.error('❌ 写入 Firebase 失败:', error.message);
      throw error;
    }
  }

  /**
   * 导出指定表到 Firebase
   */
  async exportTableToFirebase(tableName, firebasePath, options = {}) {
    const connection = this.createConnection();

    try {
      console.log(`🚀 开始导出表 ${tableName} 到 Firebase...`);

      // 1. 获取表结构
      console.log('🔍 获取表结构...');
      const structure = await this.getTableStructure(connection, tableName);

      if (structure.length === 0) {
        throw new Error(`表 ${tableName} 不存在或无法访问`);
      }

      console.log(`📋 表结构: ${structure.length} 个字段`);
      structure.forEach(column => {
        console.log(`   ${column.name} (${column.type}) ${column.pk ? 'PRIMARY KEY' : ''}`);
      });

      // 2. 获取表数据
      console.log('\n📊 获取表数据...');
      const data = await this.getTableData(connection, tableName);

      if (data.length === 0) {
        console.warn(`⚠️ 表 ${tableName} 没有数据`);
        return { successCount: 0, errorCount: 0, total: 0 };
      }

      console.log(`📈 找到 ${data.length} 条记录`);

      // 显示数据示例
      console.log('\n📄 数据示例（前3条）:');
      data.slice(0, 3).forEach((item, index) => {
        console.log(`   记录 ${index + 1}:`, JSON.stringify(item, null, 2).split('\n').slice(0, 3).join('\n') + ' ...');
      });

      // 3. 转换数据格式
      console.log('\n🔄 转换数据格式...');
      const convertedData = this.convertDataForFirebase(data);

      // 4. 自动检测主键
      const primaryKeys = structure.filter(col => col.pk).map(col => col.name);
      const autoPrimaryKey = primaryKeys.length > 0 ? primaryKeys[0] : null;

      // 5. 检查主键是否包含特殊字符
      // 移除自动检测逻辑，默认始终开启编码，除非用户显式禁用
      let encodeKeys = true;

      // 6. 写入 Firebase
      const exportOptions = {
        batchSize: options.batchSize || 100,
        primaryKey: options.primaryKey || autoPrimaryKey,
        overwrite: options.overwrite !== false, // 默认覆盖
        encodeKeys: options.encodeKeys !== false && encodeKeys // 默认编码
      };

      console.log(`\n⚙️  导出配置:`);
      console.log(`   批次大小: ${exportOptions.batchSize}`);
      console.log(`   主键: ${exportOptions.primaryKey || '无'}`);
      console.log(`   覆盖模式: ${exportOptions.overwrite}`);
      console.log(`   主键编码: ${exportOptions.encodeKeys}`);

      const result = await this.writeToFirebase(convertedData, firebasePath, exportOptions);

      console.log(`\n🎉 表 ${tableName} 导出完成!`);
      console.log(`📍 Firebase 路径: ${firebasePath}`);

      return result;

    } catch (error) {
      console.error(`❌ 导出表 ${tableName} 失败:`, error.message);
      throw error;
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 列出所有可用的表
   */
  async listTables() {
    const connection = this.createConnection();

    try {
      const tables = await this.safeQuery(connection, `
        SELECT name FROM sqlite_master 
        WHERE type='table' 
        ORDER BY name
      `);

      console.log('📋 数据库中的表:');
      tables.forEach(table => {
        console.log(`  - ${table.name}`);
      });

      return tables.map(t => t.name);

    } catch (error) {
      console.error('❌ 获取表列表失败:', error.message);
      return [];
    } finally {
      this.closeConnection(connection);
    }
  }

  /**
   * 验证 Firebase 连接
   */
  async testFirebaseConnection() {
    try {
      console.log('🔗 测试 Firebase 连接...');

      const testRef = db.ref('_test_connection');
      await testRef.set({
        timestamp: new Date().toISOString(),
        message: 'Test connection from DuckDB2Firebase'
      });

      await testRef.remove();

      console.log('✅ Firebase 连接正常');
      return true;

    } catch (error) {
      console.error('❌ Firebase 连接失败:', error.message);
      return false;
    }
  }
}

/**
 * 显示使用帮助
 */
function showHelp() {
  console.log(`
🚀 DuckDB 到 Firebase 数据导出工具

使用方法:
  node toolDuckDB2Firebase.js <tableName> <firebasePath> [options]

参数:
  tableName     要导出的 DuckDB 表名
  firebasePath  Firebase 存储路径

选项:
  --primary-key <key>   指定主键字段（默认使用表的主键）
  --batch-size <size>   批次大小（默认: 100）
  --no-overwrite        不覆盖现有数据（默认覆盖）
  --no-encode-keys      禁用主键编码（默认启用）
  --list-tables         列出所有可用的表
  --test-connection     测试 Firebase 连接
  --help                显示此帮助信息

示例:
  # 导出定期资产负债表
  node toolDuckDB2Firebase.js tblPeriodicBalanceSheet statistics/balanceSheet

  # 导出持仓汇总表，指定主键
  node toolDuckDB2Firebase.js tblHoldingAggrView statistics/holdings --primary-key ticker

  # 导出其他资产表，不覆盖现有数据
  node toolDuckDB2Firebase.js tblOtherAssets statistics/otherAssets --no-overwrite

  # 禁用主键编码（如果确定没有特殊字符）
  node toolDuckDB2Firebase.js tblHoldingAggrView statistics/holdings --no-encode-keys

  # 列出所有表
  node toolDuckDB2Firebase.js --list-tables

  # 测试连接
  node toolDuckDB2Firebase.js --test-connection
  `);
}

/**
 * 解析命令行参数
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const result = {
    tableName: null,
    firebasePath: null,
    options: {
      primaryKey: null,
      batchSize: 100,
      overwrite: true,
      encodeKeys: true
    },
    command: null
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];

    if (arg === '--list-tables') {
      result.command = 'list-tables';
    } else if (arg === '--test-connection') {
      result.command = 'test-connection';
    } else if (arg === '--help') {
      result.command = 'help';
    } else if (arg === '--primary-key') {
      result.options.primaryKey = args[++i];
    } else if (arg === '--batch-size') {
      result.options.batchSize = parseInt(args[++i]);
    } else if (arg === '--no-overwrite') {
      result.options.overwrite = false;
    } else if (arg === '--no-encode-keys') {
      result.options.encodeKeys = false;
    } else if (!result.tableName) {
      result.tableName = arg;
    } else if (!result.firebasePath) {
      result.firebasePath = arg;
    }
  }

  return result;
}

/**
 * 主函数
 */
async function main() {
  console.log('🚀 DuckDB 到 Firebase 数据导出工具启动...');

  const exporter = new DuckDBToFirebaseExporter();
  const args = parseArgs();

  try {
    // 处理命令
    if (args.command === 'help') {
      showHelp();
      return;
    } else if (args.command === 'list-tables') {
      await exporter.listTables();
      return;
    } else if (args.command === 'test-connection') {
      await exporter.testFirebaseConnection();
      return;
    }

    // 验证必要参数
    if (!args.tableName || !args.firebasePath) {
      console.error('❌ 缺少必要参数: tableName 和 firebasePath');
      showHelp();
      process.exit(1);
    }

    // 测试 Firebase 连接
    const connectionOk = await exporter.testFirebaseConnection();
    if (!connectionOk) {
      console.error('❌ Firebase 连接失败，请检查配置');
      process.exit(1);
    }

    // 执行导出
    await exporter.exportTableToFirebase(args.tableName, args.firebasePath, args.options);

  } catch (error) {
    console.error('❌ 导出过程失败:', error.message);
    process.exit(1);
  } finally {
    // 关闭 Firebase 连接
    await admin.app().delete();
  }
}

// 运行主函数
if (require.main === module) {
  main().catch(console.error);
}

module.exports = DuckDBToFirebaseExporter;
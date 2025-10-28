const { spawn } = require('child_process');
const path = require('path');
const logger = require('./logger');

// 脚本配置
const scripts = [
  {
    name: '更新Quote',
    command: 'node',
    args: ['svcUpdateQuote.js', '--immediate'],
    logFile: 'svcCalling.log'
  },
  {
    name: '更新ExchangeRate',
    command: 'node',
    args: ['svcUpdateExchangeRate.js', '--immediate'],
    logFile: 'svcCalling.log'
  },
  {
    name: '汇总计算Holding数据',
    command: 'node',
    args: ['svcHoldingAggregationTask.js', '--immediate'],
    delay: 2 * 60 * 1000, // 3分钟延迟（毫秒）
    logFile: 'svcCalling.log'
  },  
  {
    name: '资产负债表更新',
    command: 'node',
    args: ['svcPeriodicalBalanceSheetAll.js', '--immediate'],
    delay: 1 * 60 * 1000, // 3分钟延迟（毫秒）
    logFile: 'svcCalling.log'
  },
  {
    name: 'tblHoldingAggrView导出到Firebase',
    command: 'node', 
    args: ['toolDuckDB2Firebase.js', 'tblHoldingAggrView', 'reports/holdings', '--no-key-check'],
    delay: 2 * 60 * 1000, // 2分钟延迟（毫秒）
    logFile: 'firebase-export.log'
  },
  {
    name: '数据导出到Firebase',
    command: 'node', 
    args: ['toolDuckDB2Firebase.js', 'tblPeriodicBalanceSheet', 'reports/balanceSheet'],
    delay: 2 * 60 * 1000, // 2分钟延迟（毫秒）
    logFile: 'firebase-export.log'
  }

];

function runScript(script) {
  return new Promise((resolve, reject) => {
    const startTime = Date.now();
    const scriptLogger = logger.child({ 
      script: script.name,
      command: `${script.command} ${script.args.join(' ')}`
    });

    scriptLogger.info('🚀 开始执行脚本');
    scriptLogger.debug(`工作目录: ${__dirname}`);
    scriptLogger.debug(`完整命令: ${script.command} ${script.args.join(' ')}`);

    const child = spawn(script.command, script.args, {
      stdio: ['pipe', 'pipe', 'pipe'], // 分离 stdio 以便记录
      cwd: __dirname,
      env: { ...process.env, NODE_ENV: 'production' }
    });

    let stdoutData = '';
    let stderrData = '';

    // 捕获标准输出
    child.stdout.on('data', (data) => {
      const output = data.toString().trim();
      stdoutData += output + '\n';
      scriptLogger.info(`STDOUT: ${output}`);
    });

    // 捕获错误输出
    child.stderr.on('data', (data) => {
      const output = data.toString().trim();
      stderrData += output + '\n';
      scriptLogger.error(`STDERR: ${output}`);
    });

    child.on('close', (code) => {
      const executionTime = ((Date.now() - startTime) / 1000).toFixed(2);
      
      if (code === 0) {
        scriptLogger.info(`✅ 脚本执行成功 - 耗时: ${executionTime}秒`);
        
        // 记录详细的执行摘要
        if (stdoutData) {
          scriptLogger.debug('执行输出摘要:', { 
            outputLines: stdoutData.split('\n').length 
          });
        }
        
        resolve({
          success: true,
          executionTime,
          output: stdoutData
        });
      } else {
        scriptLogger.error(`❌ 脚本执行失败 - 退出码: ${code}, 耗时: ${executionTime}秒`);
        scriptLogger.error('错误输出:', { stderr: stderrData });
        
        reject(new Error(`脚本退出码: ${code}, 错误: ${stderrData || '未知错误'}`));
      }
    });
    
    child.on('error', (error) => {
      scriptLogger.error(`💥 脚本执行错误: ${error.message}`, { error: error.stack });
      reject(error);
    });
  });
}

function delay(ms) {
  const minutes = ms / 60000;
  logger.info(`⏰ 等待 ${minutes} 分钟...`);
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function runAllScripts() {
  const runId = Date.now();
  const runLogger = logger.child({ runId });
  
  runLogger.info('='.repeat(60));
  runLogger.info('📅 开始执行每日报表任务序列');
  runLogger.info(`⏰ 开始时间: ${new Date().toLocaleString('zh-CN')}`);
  runLogger.info(`📝 任务数量: ${scripts.length}`);
  runLogger.info('='.repeat(60));

  try {
    for (let i = 0; i < scripts.length; i++) {
      const script = scripts[i];
      const scriptLogger = logger.child({ 
        script: script.name,
        sequence: i + 1
      });
      
      scriptLogger.info(`🔄 准备执行任务 (${i + 1}/${scripts.length})`);
      
      // 执行当前脚本
      const result = await runScript(script);
      
      scriptLogger.info(`✓ 任务完成 - 耗时: ${result.executionTime}秒`);
      
      // 如果不是最后一个脚本，且设置了延迟，则等待
      if (i < scripts.length - 1 && scripts[i + 1].delay) {
        const nextScript = scripts[i + 1];
        scriptLogger.info(`⏳ 下一个任务: ${nextScript.name} (${nextScript.delay/60000}分钟后)`);
        await delay(scripts[i + 1].delay);
      }
    }
    
    runLogger.info('='.repeat(60));
    runLogger.info('🎉 所有任务执行完成！');
    runLogger.info(`⏰ 结束时间: ${new Date().toLocaleString('zh-CN')}`);
    runLogger.info('='.repeat(60));
    
    process.exit(0);
    
  } catch (error) {
    logger.error('💥 任务序列执行失败', {
      error: error.message,
      stack: error.stack
    });
    
    logger.error('='.repeat(60));
    logger.error('❌ 任务执行中断');
    logger.error(`⏰ 失败时间: ${new Date().toLocaleString('zh-CN')}`);
    logger.error('='.repeat(60));
    
    process.exit(1);
  }
}

// 错误处理
process.on('unhandledRejection', (reason, promise) => {
  logger.error('⚠️ 未处理的 Promise 拒绝:', {
    reason: reason instanceof Error ? reason.stack : reason,
    promise
  });
});

process.on('uncaughtException', (error) => {
  logger.error('💥 未捕获的异常:', {
    error: error.stack,
    message: error.message
  });
  process.exit(1);
});

// 启动执行
if (require.main === module) {
  runAllScripts();
}

module.exports = { runAllScripts, runScript };
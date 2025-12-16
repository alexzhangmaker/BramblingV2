// diagnose-logging.js
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

async function diagnoseLogging() {
  console.log('🔍 诊断日志记录问题...\n');
  
  const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_ANON_KEY
  );

  // 登录
  const { data: authData, error: authError } = await supabase.auth.signInWithPassword({
    email: process.env.SERVICE_ACCOUNT_EMAIL,
    password: process.env.SERVICE_ACCOUNT_PASSWORD
  });

  if (authError) {
    console.log('❌ 登录失败:', authError.message);
    return;
  }

  console.log('✅ 登录成功:', authData.user.email);
  console.log('用户ID:', authData.user.id);

  // 测试1: 检查 service_accounts 表
  console.log('\n1. 检查 service_accounts 表...');
  const { data: serviceAccount, error: saError } = await supabase
    .from('service_accounts')
    .select('*')
    .eq('user_id', authData.user.id)
    .single();

  if (saError) {
    console.log('❌ 查询 service_accounts 失败:', saError.message);
  } else if (serviceAccount) {
    console.log('✅ 找到 service_account 记录:', serviceAccount.name);
  } else {
    console.log('❌ 在 service_accounts 表中未找到用户记录');
  }

  // 测试2: 检查其他表的访问
  console.log('\n2. 测试其他表访问...');
  const { data: deals, error: dealsError } = await supabase
    .from('dealLogs')
    .select('count')
    .limit(1);

  console.log('dealLogs 表访问:', dealsError ? '❌ 失败 - ' + dealsError.message : '✅ 成功');

  const { data: account, error: accountError } = await supabase
    .from('account_IB7075')
    .select('count')
    .limit(1);

  console.log('account_IB7075 表访问:', accountError ? '❌ 失败 - ' + accountError.message : '✅ 成功');

  // 测试3: 测试 operation_logs 表插入
  console.log('\n3. 测试 operation_logs 表插入...');
  const testLog = {
    operation_type: 'diagnostic_test',
    operation_target: 'diagnostic',
    target_record_id: 'test-' + Date.now(),
    status: 'SUCCESS',
    executed_by: 'diagnostic-script'
  };

  const { data: logData, error: logError } = await supabase
    .from('operation_logs')
    .insert(testLog)
    .select();

  if (logError) {
    console.log('❌ operation_logs 插入失败:');
    console.log('   错误代码:', logError.code);
    console.log('   错误信息:', logError.message);
    console.log('   详细信息:', logError.details);
    console.log('   提示:', logError.hint);
  } else {
    console.log('✅ operation_logs 插入成功!');
    console.log('   记录ID:', logData[0].log_id);
  }

  // 测试4: 检查现有日志
  console.log('\n4. 检查现有日志记录...');
  const { data: existingLogs, error: logsError } = await supabase
    .from('operation_logs')
    .select('log_id, operation_type, executed_at')
    .order('executed_at', { ascending: false })
    .limit(5);

  if (logsError) {
    console.log('❌ 查询现有日志失败:', logsError.message);
  } else {
    console.log(`✅ 找到 ${existingLogs.length} 条现有日志记录`);
  }
}

diagnoseLogging();
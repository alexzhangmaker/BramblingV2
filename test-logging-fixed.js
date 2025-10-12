// test-logging-fixed.js
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

async function testLoggingFixed() {
  console.log('🧪 测试修复后的日志记录...\n');
  
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

  // 测试日志记录
  const testLog = {
    operation_type: 'test_after_fix',
    operation_target: 'test',
    target_record_id: 'test-' + Date.now(),
    status: 'SUCCESS',
    executed_by: 'test-script',
    operation_data: { test: true, timestamp: new Date().toISOString() }
  };

  console.log('尝试插入日志记录...');
  const { data, error } = await supabase
    .from('operation_logs')
    .insert(testLog)
    .select();

  if (error) {
    console.log('❌ 日志记录仍然失败:');
    console.log('   错误代码:', error.code);
    console.log('   错误信息:', error.message);
    
    // 如果还是失败，使用方案3
    console.log('\n💡 建议使用方案3：只允许 Service Account 访问');
  } else {
    console.log('✅ 日志记录成功!');
    console.log('   记录ID:', data[0].log_id);
    
    // 测试查询
    console.log('\n测试日志查询...');
    const { data: logs, error: queryError } = await supabase
      .from('operation_logs')
      .select('log_id, operation_type, executed_at')
      .order('executed_at', { ascending: false })
      .limit(3);
    
    if (queryError) {
      console.log('❌ 日志查询失败:', queryError.message);
    } else {
      console.log(`✅ 日志查询成功，找到 ${logs.length} 条记录`);
    }
  }
}

testLoggingFixed();
// deep-diagnose.js
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

async function deepDiagnose() {
  console.log('🔍 深入诊断权限问题...\n');
  
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

  // 测试1: 直接查询 service_accounts 表
  console.log('\n1. 直接查询 service_accounts 表...');
  const { data: saData, error: saError } = await supabase
    .from('service_accounts')
    .select('*')
    .eq('user_id', authData.user.id);

  if (saError) {
    console.log('❌ 查询 service_accounts 失败:');
    console.log('   错误代码:', saError.code);
    console.log('   错误信息:', saError.message);
    console.log('   这证实了问题所在！');
  } else {
    console.log('✅ service_accounts 查询成功');
    console.log('   找到记录:', saData.length, '条');
  }

  // 测试2: 检查 service_accounts 表的 RLS 策略
  console.log('\n2. 检查 service_accounts 表的 RLS 状态...');
  const { data: saTable, error: saTableError } = await supabase
    .from('service_accounts')
    .select('*')
    .limit(1);

  if (saTableError) {
    console.log('❌ service_accounts 表访问被拒绝:', saTableError.message);
  } else {
    console.log('✅ service_accounts 表可以访问');
  }

  // 测试3: 测试最简单的日志策略
  console.log('\n3. 测试最简单的策略...');
}

deepDiagnose();
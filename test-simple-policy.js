// test-simple-policy.js
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

async function testSimplePolicy() {
  console.log('🧪 测试简单策略...\n');
  
  const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_ANON_KEY
  );

  const { data: authData, error: authError } = await supabase.auth.signInWithPassword({
    email: process.env.SERVICE_ACCOUNT_EMAIL,
    password: process.env.SERVICE_ACCOUNT_PASSWORD
  });

  if (authError) {
    console.log('❌ 登录失败:', authError.message);
    return;
  }

  console.log('✅ 登录成功, 用户ID:', authData.user.id);

  const { data, error } = await supabase
    .from('operation_logs')
    .insert({
      operation_type: 'simple_policy_test',
      operation_target: 'test',
      target_record_id: 'simple-test'
    })
    .select();

  if (error) {
    console.log('❌ 简单策略失败:', error.message);
    console.log('当前用户ID:', authData.user.id);
    console.log('策略检查的用户ID: adf97a04-29f9-40ea-954d-1e211271f2fc');
    console.log('是否匹配:', authData.user.id === 'adf97a04-29f9-40ea-954d-1e211271f2fc');
  } else {
    console.log('✅ 简单策略成功! 记录ID:', data[0].log_id);
    
    // 如果成功，升级到正式策略
    console.log('\n🎉 简单策略成功，现在创建正式策略...');
    console.log(`
请在 Supabase SQL Editor 中执行：
DROP POLICY IF EXISTS "simple_user_access" ON operation_logs;

CREATE POLICY "service_account_access_operation_logs" ON operation_logs
FOR ALL USING (
  EXISTS (SELECT 1 FROM service_accounts WHERE user_id = auth.uid())
);
    `);
  }
}

testSimplePolicy();
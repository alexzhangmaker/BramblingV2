// test-service-account-fixed.js
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

// 初始化客户端
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

async function testServiceAccount() {
  console.log('🔐 开始测试 Service Account 访问...\n');

  try {
    // 1. 使用 Service Account 登录
    console.log('1. 正在登录 Service Account...');
    const { data: authData, error: authError } = await supabase.auth.signInWithPassword({
      email: process.env.SERVICE_ACCOUNT_EMAIL,
      password: process.env.SERVICE_ACCOUNT_PASSWORD
    });

    if (authError) {
      throw new Error(`登录失败: ${authError.message}`);
    }

    console.log('✅ 登录成功!');
    console.log(`   用户: ${authData.user.email}`);
    console.log(`   用户ID: ${authData.user.id}\n`);

    // 2. 首先检查 service_accounts 表中是否有当前用户
    console.log('2. 检查 Service Account 权限...');
    const { data: serviceAccountCheck, error: checkError } = await supabase
      .from('service_accounts')
      .select('user_id, name')
      .eq('user_id', authData.user.id)
      .single();

    if (checkError || !serviceAccountCheck) {
      console.log('❌ Service Account 未在 service_accounts 表中注册');
      console.log('💡 请在 service_accounts 表中插入以下记录:');
      console.log(`   INSERT INTO service_accounts (user_id, name) VALUES ('${authData.user.id}', 'nodejs-backend');`);
      
      // 继续测试，看看 Gmail 策略是否生效
      console.log('🔄 继续测试 Gmail 策略...\n');
    } else {
      console.log('✅ Service Account 权限确认!');
      console.log(`   名称: ${serviceAccountCheck.name}\n`);
    }

    // 3. 测试读取数据 (SELECT)
    console.log('3. 测试读取数据...');
    const { data: readData, error: readError } = await supabase
      .from('posts')
      .select('*')
      .limit(3);

    if (readError) {
      if (readError.code === '42501') {
        throw new Error(`读取权限被拒绝: 请检查 RLS 策略。当前用户: ${authData.user.email}`);
      } else {
        throw new Error(`读取失败: ${readError.message} (代码: ${readError.code})`);
      }
    }

    console.log('✅ 读取成功!');
    console.log(`   获取到 ${readData.length} 条记录`);
    if (readData.length > 0) {
      readData.forEach((post, index) => {
        console.log(`   ${index + 1}. ID: ${post.id}, 标题: ${post.title}`);
      });
    } else {
      console.log('   📝 表中暂无数据，将进行写入测试...');
    }
    console.log('');

    // 4. 测试写入数据 (INSERT) - 只在表为空或需要测试时进行
    console.log('4. 测试写入数据...');
    const testData = {
      title: `Service Account 测试文章 - ${new Date().toLocaleString()}`,
      content: '这是通过 Service Account 自动创建的内容',
      author_id: authData.user.id,
      is_published: true
    };

    const { data: insertData, error: insertError } = await supabase
      .from('posts')
      .insert(testData)
      .select();

    if (insertError) {
      if (insertError.code === '42501') {
        throw new Error(`写入权限被拒绝: 请检查 RLS 策略。当前用户邮箱: ${authData.user.email}`);
      } else {
        throw new Error(`写入失败: ${insertError.message} (代码: ${insertError.code})`);
      }
    }

    console.log('✅ 写入成功!');
    console.log(`   创建记录ID: ${insertData[0].id}`);
    console.log(`   标题: ${insertData[0].title}\n`);

    // 5. 测试更新数据 (UPDATE)
    console.log('5. 测试更新数据...');
    const { data: updateData, error: updateError } = await supabase
      .from('posts')
      .update({ 
        title: `已更新 - ${testData.title}`,
        updated_at: new Date().toISOString()
      })
      .eq('id', insertData[0].id)
      .select();

    if (updateError) {
      throw new Error(`更新失败: ${updateError.message}`);
    }

    console.log('✅ 更新成功!');
    console.log(`   新标题: ${updateData[0].title}\n`);

    // 6. 测试删除数据 (DELETE)
    console.log('6. 测试删除数据...');
    const { error: deleteError } = await supabase
      .from('posts')
      .delete()
      .eq('id', insertData[0].id);

    if (deleteError) {
      console.log('⚠️  删除失败:', deleteError.message);
      console.log('💡 这可能是因为 DELETE 策略限制，但其他操作正常即可');
    } else {
      console.log('✅ 删除成功!');
    }

    console.log('\n🎉 Service Account 测试完成!');

  } catch (error) {
    console.error('\n❌ 测试失败:');
    console.error('   错误信息:', error.message);
    
    // 提供具体的调试建议
    console.log('\n🔧 具体调试步骤:');
    console.log('   1. 在 Supabase SQL Editor 中执行修复 SQL');
    console.log('   2. 确保 service_accounts 表中有你的用户ID');
    console.log('   3. 检查 posts 表的 RLS 策略是否正确');
    console.log('   4. 确认用户邮箱是 Gmail 域名');
    
    console.log('\n📋 立即执行的 SQL 解决方案:');
    console.log(`
-- 在 service_accounts 表中注册用户
INSERT INTO service_accounts (user_id, name) 
VALUES ('adf97a04-29f9-40ea-954d-1e211271f2fc', 'nodejs-backend')
ON CONFLICT (user_id) DO UPDATE SET name = 'nodejs-backend';

-- 检查策略
SELECT schemaname, tablename, policyname, qual 
FROM pg_policies 
WHERE tablename = 'posts';
    `);
  }
}

// 运行测试
testServiceAccount();
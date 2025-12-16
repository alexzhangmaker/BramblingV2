// jsCreatePostTbl.js
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

// 检查环境变量
console.log('检查环境变量...');
console.log('SUPABASE_URL:', process.env.SUPABASE_URL ? '已设置' : '未设置');
console.log('SUPABASE_ANON_KEY:', process.env.SUPABASE_ANON_KEY ? '已设置' : '未设置');

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_ANON_KEY) {
  console.error('❌ 错误: 请确保 .env 文件中设置了 SUPABASE_URL 和 SUPABASE_ANON_KEY');
  process.exit(1);
}

// 使用 anon key 初始化客户端
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

async function createTableWithSQL() {
  console.log('📋 请在 Supabase SQL Editor 中执行以下 SQL 语句:\n');
  
  const sql = `
-- 创建 posts 表
CREATE TABLE IF NOT EXISTS posts (
  id BIGSERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  content TEXT,
  author_id UUID REFERENCES auth.users(id),
  is_published BOOLEAN DEFAULT false,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 启用 RLS
ALTER TABLE posts ENABLE ROW LEVEL SECURITY;

-- 为 Service Account 创建完整访问策略
CREATE POLICY "service_account_full_access_posts" ON posts
FOR ALL USING (
  EXISTS (
    SELECT 1 FROM service_accounts 
    WHERE user_id = auth.uid()
  )
);

-- 为 Gmail 用户创建完整访问策略  
CREATE POLICY "gmail_users_full_access_posts" ON posts
FOR ALL USING (
  (SELECT email FROM auth.users WHERE id = auth.uid()) LIKE '%@gmail.com'
);

-- 插入一些测试数据（可选）
INSERT INTO posts (title, content, author_id, is_published) VALUES
('第一篇测试文章', '这是第一篇测试文章的内容', '00000000-0000-0000-0000-000000000000', true),
('第二篇测试文章', '这是第二篇测试文章的内容', '00000000-0000-0000-0000-000000000000', false)
ON CONFLICT DO NOTHING;
`;

  console.log(sql);
  console.log('\n✨ 请将上面的 SQL 复制到 Supabase SQL Editor 中执行');
}

// 运行函数
createTableWithSQL();
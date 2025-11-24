// config.js
const SUPABASE_CONFIG = {
  url: 'https://czynxewwitqqnfvxstpy.supabase.co',
  anonKey: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImN6eW54ZXd3aXRxcW5mdnhzdHB5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTk3ODUwMzgsImV4cCI6MjA3NTM2MTAzOH0.PLVb4RwdhQmsxds53VYYR_Z_mnt3tJtEuqrJ5Ge--Bw'
};

// 导出配置以便其他文件使用
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { SUPABASE_CONFIG };
}
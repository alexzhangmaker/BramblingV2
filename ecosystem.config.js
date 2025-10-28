// ecosystem.config.js
module.exports = {
  apps: [{
    name: 'smart-scheduler',
    script: './mainBrambling.js',
    instances: 1,
    autorestart: true, // 改为 true
    watch: false,
    env: {
      NODE_ENV: 'production'
    }
  }]
};
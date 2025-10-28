const winston = require('winston');
const path = require('path');

// 创建日志目录
const logDir = path.join(__dirname, 'logs');

// 自定义日志格式
const logFormat = winston.format.combine(
  winston.format.timestamp({
    format: 'YYYY-MM-DD HH:mm:ss'
  }),
  winston.format.errors({ stack: true }),
  winston.format.printf(({ level, message, timestamp, stack }) => {
    if (stack) {
      return `${timestamp} [${level.toUpperCase()}]: ${message}\n${stack}`;
    }
    return `${timestamp} [${level.toUpperCase()}]: ${message}`;
  })
);

// 创建 logger 实例
const logger = winston.createLogger({
  level: 'info',
  format: logFormat,
  defaultMeta: { service: 'script-runner' },
  transports: [
    // 文件传输 - 所有日志
    new winston.transports.File({
      filename: path.join(logDir, 'combined.log'),
      level: 'info',
      maxsize: 10485760, // 10MB
      maxFiles: 5
    }),
    // 文件传输 - 错误日志
    new winston.transports.File({
      filename: path.join(logDir, 'errors.log'),
      level: 'error',
      maxsize: 10485760, // 10MB
      maxFiles: 5
    }),
    // 控制台输出 - 用于 PM2 日志收集
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.simple()
      )
    })
  ]
});

// 如果是开发环境，增加调试级别
if (process.env.NODE_ENV !== 'production') {
  logger.add(new winston.transports.Console({
    level: 'debug',
    format: winston.format.combine(
      winston.format.colorize(),
      winston.format.simple()
    )
  }));
}

module.exports = logger;
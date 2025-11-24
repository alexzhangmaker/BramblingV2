// dbLocker.js
const logger = require('./logger');

class DBLocker {
  constructor() {
    this.locks = new Map();
    this.timeouts = new Map();
  }

  /**
   * 获取锁
   * @param {string} lockName 锁名称
   * @param {number} timeoutMs 超时时间（毫秒）
   * @returns {Promise<boolean>} 是否成功获取锁
   */
  async acquireLock(lockName, timeoutMs = 5 * 60 * 1000) {
    if (this.locks.has(lockName)) {
      logger.warn(`⚠️ 锁 "${lockName}" 已被占用，跳过执行`);
      return false;
    }

    this.locks.set(lockName, Date.now());
    
    // 设置超时自动释放
    const timeout = setTimeout(() => {
      logger.warn(`⏰ 锁 "${lockName}" 超时自动释放`);
      this.releaseLock(lockName);
    }, timeoutMs);

    this.timeouts.set(lockName, timeout);
    
    logger.info(`🔒 获取锁: ${lockName} (超时: ${timeoutMs / 1000}秒)`);
    return true;
  }

  /**
   * 释放锁
   * @param {string} lockName 锁名称
   */
  releaseLock(lockName) {
    if (this.timeouts.has(lockName)) {
      clearTimeout(this.timeouts.get(lockName));
      this.timeouts.delete(lockName);
    }
    
    if (this.locks.has(lockName)) {
      this.locks.delete(lockName);
      logger.info(`🔓 释放锁: ${lockName}`);
    }
  }

  /**
   * 检查锁是否存在
   * @param {string} lockName 锁名称
   * @returns {boolean} 是否存在锁
   */
  hasLock(lockName) {
    return this.locks.has(lockName);
  }
}

module.exports = new DBLocker();
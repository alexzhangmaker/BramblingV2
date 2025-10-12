// simple-logger.js
class SimpleOperationLogger {
  constructor(supabaseClient) {
    this.supabase = supabaseClient;
  }

  // 记录操作日志
  async logOperation(operationInfo) {
    const logRecord = {
      operation_type: operationInfo.type,
      operation_target: operationInfo.target,
      target_record_id: operationInfo.recordId,
      operation_data: operationInfo.data || null,
      status: operationInfo.status,
      error_message: operationInfo.error || null,
      executed_by: operationInfo.executedBy || 'system',
      executed_at: new Date().toISOString(),
      duration_ms: operationInfo.duration || 0
    };

    try {
      const { data, error } = await this.supabase
        .from('operation_logs')
        .insert(logRecord)
        .select()
        .single();

      if (error) {
        console.error('日志记录失败:', error);
        // 如果数据库日志失败，至少输出到控制台
        console.log('操作日志(控制台):', logRecord);
        return null;
      }

      return data;
    } catch (error) {
      console.error('日志记录异常:', error);
      console.log('操作日志(控制台):', logRecord);
      return null;
    }
  }

  // 快速记录成功操作
  async logSuccess(operationType, target, recordId, data = null) {
    return await this.logOperation({
      type: operationType,
      target: target,
      recordId: recordId,
      data: data,
      status: 'SUCCESS'
    });
  }

  // 快速记录失败操作
  async logFailure(operationType, target, recordId, error, data = null) {
    return await this.logOperation({
      type: operationType,
      target: target,
      recordId: recordId,
      data: data,
      status: 'FAILED',
      error: error.message || error.toString()
    });
  }

  // 查询操作日志
  async getOperationLogs(filters = {}, limit = 100) {
    let query = this.supabase
      .from('operation_logs')
      .select('*')
      .order('executed_at', { ascending: false })
      .limit(limit);

    if (filters.operationType) {
      query = query.eq('operation_type', filters.operationType);
    }
    if (filters.target) {
      query = query.eq('operation_target', filters.target);
    }
    if (filters.status) {
      query = query.eq('status', filters.status);
    }
    if (filters.startDate) {
      query = query.gte('executed_at', filters.startDate);
    }
    if (filters.endDate) {
      query = query.lte('executed_at', filters.endDate);
    }

    const { data, error } = await query;

    if (error) {
      console.error('查询操作日志失败:', error);
      return [];
    }

    return data;
  }

  // 获取操作统计
  async getOperationStats(days = 7) {
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - days);

    const { data, error } = await this.supabase
      .from('operation_logs')
      .select('operation_type, status, executed_at')
      .gte('executed_at', startDate.toISOString());

    if (error) {
      console.error('获取操作统计失败:', error);
      return null;
    }

    const stats = {
      total: data.length,
      byType: {},
      byStatus: {},
      successRate: 0
    };

    data.forEach(log => {
      // 按操作类型统计
      stats.byType[log.operation_type] = (stats.byType[log.operation_type] || 0) + 1;
      
      // 按状态统计
      stats.byStatus[log.status] = (stats.byStatus[log.status] || 0) + 1;
    });

    const successCount = stats.byStatus.SUCCESS || 0;
    stats.successRate = stats.total > 0 ? (successCount / stats.total) * 100 : 0;

    return stats;
  }
}

module.exports = SimpleOperationLogger;
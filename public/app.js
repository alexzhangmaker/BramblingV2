// app.js
// app.js
let supabase = null;
let currentUser = null;
let currentPage = 1;
const pageSize = 20;
let allDealLogs = [];
let filteredDealLogs = [];

// 初始化应用
async function initApp() {
    try {
        console.log('🚀 初始化应用...');
        
        // 检查配置
        if (!SUPABASE_CONFIG.url || !SUPABASE_CONFIG.anonKey) {
            showError('请先在 config.js 中配置 Supabase URL 和 Anon Key');
            return;
        }

        // 初始化 Supabase 客户端
        supabase = window.supabase.createClient(SUPABASE_CONFIG.url, SUPABASE_CONFIG.anonKey);
        console.log('✅ Supabase 客户端初始化成功');
        
        // 检查是否有已保存的会话
        const { data: { session }, error: sessionError } = await supabase.auth.getSession();
        
        if (sessionError) {
            console.error('会话检查错误:', sessionError);
            showLoginForm();
            return;
        }
        
        if (session) {
            console.log('✅ 找到已保存的会话');
            currentUser = session.user;
            showUserInfo();
            showContent();
            await loadDealLogs();
        } else {
            console.log('ℹ️ 没有找到已保存的会话');
            showLoginForm();
        }
        
        // 监听认证状态变化
        supabase.auth.onAuthStateChange((event, session) => {
            console.log('认证状态变化:', event);
            if (event === 'SIGNED_IN' && session) {
                currentUser = session.user;
                showUserInfo();
                showContent();
                loadDealLogs();
            } else if (event === 'SIGNED_OUT') {
                currentUser = null;
                showLoginForm();
                hideContent();
            }
        });
        
    } catch (error) {
        console.error('初始化失败:', error);
        showError('初始化失败: ' + error.message);
    }
}
// 登录函数
async function login() {
    const email = document.getElementById('email').value;
    const password = document.getElementById('password').value;
    
    if (!email || !password) {
        showError('请输入邮箱和密码');
        return;
    }
    
    // 检查是否是 Gmail 邮箱
    if (!email.endsWith('@gmail.com')) {
        showError('只允许 Gmail 邮箱登录');
        return;
    }
    
    showLoading(true);
    
    try {
        const { data, error } = await supabase.auth.signInWithPassword({
            email: email,
            password: password
        });
        
        if (error) throw error;
        
        currentUser = data.user;
        showUserInfo();
        showContent();
        await loadDealLogs();
        showSuccess('登录成功！');
        
    } catch (error) {
        showError('登录失败: ' + error.message);
    } finally {
        showLoading(false);
    }
}

// 退出登录
async function logout() {
    const { error } = await supabase.auth.signOut();
    if (error) {
        showError('退出失败: ' + error.message);
    }
}

// 显示/隐藏界面元素
function showLoginForm() {
    document.getElementById('login-form').style.display = 'flex';
    document.getElementById('user-info').style.display = 'none';
}

function showUserInfo() {
    document.getElementById('login-form').style.display = 'none';
    document.getElementById('user-info').style.display = 'flex';
    document.getElementById('user-email').textContent = currentUser.email;
}

function showContent() {
    document.getElementById('content').style.display = 'block';
}

function hideContent() {
    document.getElementById('content').style.display = 'none';
}

function showLoading(show) {
    document.getElementById('loading').style.display = show ? 'block' : 'none';
}

function showError(message) {
    const errorEl = document.getElementById('error-message');
    errorEl.textContent = message;
    errorEl.style.display = 'block';
    setTimeout(() => errorEl.style.display = 'none', 5000);
}

function showSuccess(message) {
    const successEl = document.createElement('div');
    successEl.className = 'success';
    successEl.textContent = message;
    document.querySelector('.container').insertBefore(successEl, document.getElementById('content'));
    setTimeout(() => successEl.remove(), 3000);
}

// 加载交易记录
async function loadDealLogs() {
    showLoading(true);
    
    try {
        const { data, error } = await supabase
            .from('dealLogs')
            .select('*')
            .order('dealID', { ascending: false });
        
        if (error) throw error;
        
        allDealLogs = data;
        filteredDealLogs = [...allDealLogs];
        renderDealLogs();
        updateStats();
        updateFilters();
        
    } catch (error) {
        showError('加载数据失败: ' + error.message);
    } finally {
        showLoading(false);
    }
}

// 渲染交易记录表格
function renderDealLogs() {
    const tbody = document.getElementById('deal-logs-body');
    const startIndex = (currentPage - 1) * pageSize;
    const endIndex = startIndex + pageSize;
    const pageData = filteredDealLogs.slice(startIndex, endIndex);
    
    tbody.innerHTML = '';
    
    if (pageData.length === 0) {
        tbody.innerHTML = '<tr><td colspan="10" style="text-align: center;">没有找到交易记录</td></tr>';
        return;
    }
    
    pageData.forEach(deal => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${deal.dealID}</td>
            <td>${deal.account || '-'}</td>
            <td class="action-${deal.action.toLowerCase()}">${deal.action}</td>
            <td>${deal.ticker || '-'}</td>
            <td>$${deal.price ? deal.price.toFixed(2) : '0.00'}</td>
            <td>${deal.quantity || 0}</td>
            <td>${deal.market || '-'}</td>
            <td>${deal.date || '-'}</td>
            <td class="status-${deal.cleared ? 'cleared' : 'pending'}">
                ${deal.cleared ? '已清算' : '未清算'}
            </td>
            <td>
                <button class="action-btn btn-edit" onclick="editDealLog(${deal.dealID})">编辑</button>
                <button class="action-btn btn-delete" onclick="deleteDealLog(${deal.dealID})">删除</button>
            </td>
        `;
        tbody.appendChild(row);
    });
    
    updatePagination();
}

// 更新统计信息
function updateStats() {
    const stats = document.getElementById('stats');
    const total = allDealLogs.length;
    const buyCount = allDealLogs.filter(d => d.action === 'BUY').length;
    const sellCount = allDealLogs.filter(d => d.action === 'SELL').length;
    const clearedCount = allDealLogs.filter(d => d.cleared).length;
    const totalValue = allDealLogs.reduce((sum, deal) => sum + (deal.price * deal.quantity), 0);
    
    stats.innerHTML = `
        <div class="stat-item">总记录: ${total}</div>
        <div class="stat-item">买入: ${buyCount}</div>
        <div class="stat-item">卖出: ${sellCount}</div>
        <div class="stat-item">已清算: ${clearedCount}</div>
        <div class="stat-item">总价值: $${totalValue.toFixed(2)}</div>
    `;
}

// 更新过滤器选项
function updateFilters() {
    const accounts = [...new Set(allDealLogs.map(d => d.account).filter(Boolean))];
    const accountSelect = document.getElementById('filter-account');
    
    accountSelect.innerHTML = '<option value="">所有账户</option>';
    accounts.forEach(account => {
        accountSelect.innerHTML += `<option value="${account}">${account}</option>`;
    });
}

// 应用过滤器
function applyFilters() {
    const accountFilter = document.getElementById('filter-account').value;
    const actionFilter = document.getElementById('filter-action').value;
    const clearedFilter = document.getElementById('filter-cleared').value;
    const tickerFilter = document.getElementById('filter-ticker').value.toLowerCase();
    
    filteredDealLogs = allDealLogs.filter(deal => {
        return (!accountFilter || deal.account === accountFilter) &&
               (!actionFilter || deal.action === actionFilter) &&
               (clearedFilter === '' || deal.cleared === (clearedFilter === 'true')) &&
               (!tickerFilter || (deal.ticker && deal.ticker.toLowerCase().includes(tickerFilter)));
    });
    
    currentPage = 1;
    renderDealLogs();
}

// 分页功能
function updatePagination() {
    const totalPages = Math.ceil(filteredDealLogs.length / pageSize);
    document.getElementById('page-info').textContent = `第 ${currentPage} 页，共 ${totalPages} 页`;
    document.getElementById('prev-page').disabled = currentPage === 1;
    document.getElementById('next-page').disabled = currentPage === totalPages || totalPages === 0;
}

function changePage(direction) {
    const totalPages = Math.ceil(filteredDealLogs.length / pageSize);
    const newPage = currentPage + direction;
    
    if (newPage >= 1 && newPage <= totalPages) {
        currentPage = newPage;
        renderDealLogs();
    }
}

// 添加记录功能
function showAddForm() {
    document.getElementById('add-form').style.display = 'block';
    document.getElementById('add-date').value = new Date().toISOString().split('T')[0];
}

function hideAddForm() {
    document.getElementById('add-form').style.display = 'none';
    // 清空表单
    document.getElementById('add-form').querySelectorAll('input, select').forEach(el => {
        if (el.type !== 'button' && el.type !== 'submit') {
            el.value = '';
        }
    });
}

async function addDealLog() {
    const dealData = {
        account: document.getElementById('add-account').value,
        action: document.getElementById('add-action').value,
        ticker: document.getElementById('add-ticker').value,
        price: parseFloat(document.getElementById('add-price').value) || 0,
        quantity: parseInt(document.getElementById('add-quantity').value) || 0,
        market: document.getElementById('add-market').value,
        date: document.getElementById('add-date').value,
        cleared: document.getElementById('add-cleared').checked
    };
    
    // 简单验证
    if (!dealData.ticker || !dealData.account) {
        showError('请填写代码和账户');
        return;
    }
    
    showLoading(true);
    
    try {
        const { data, error } = await supabase
            .from('dealLogs')
            .insert(dealData)
            .select();
        
        if (error) throw error;
        
        hideAddForm();
        await loadDealLogs();
        showSuccess('交易记录添加成功！');
        
    } catch (error) {
        showError('添加失败: ' + error.message);
    } finally {
        showLoading(false);
    }
}

// 编辑和删除功能（简化版）
async function editDealLog(dealID) {
    const newPrice = prompt('请输入新的价格:');
    if (newPrice && !isNaN(parseFloat(newPrice))) {
        try {
            const { error } = await supabase
                .from('dealLogs')
                .update({ price: parseFloat(newPrice) })
                .eq('dealID', dealID);
            
            if (error) throw error;
            
            await loadDealLogs();
            showSuccess('更新成功！');
            
        } catch (error) {
            showError('更新失败: ' + error.message);
        }
    }
}

async function deleteDealLog(dealID) {
    if (confirm('确定要删除这条记录吗？')) {
        try {
            const { error } = await supabase
                .from('dealLogs')
                .delete()
                .eq('dealID', dealID);
            
            if (error) throw error;
            
            await loadDealLogs();
            showSuccess('删除成功！');
            
        } catch (error) {
            showError('删除失败: ' + error.message);
        }
    }
}

// 导出 CSV
function exportToCSV() {
    const headers = ['ID', '账户', '操作', '代码', '价格', '数量', '市场', '日期', '状态'];
    const csvData = filteredDealLogs.map(deal => [
        deal.dealID,
        deal.account,
        deal.action,
        deal.ticker,
        deal.price,
        deal.quantity,
        deal.market,
        deal.date,
        deal.cleared ? '已清算' : '未清算'
    ]);
    
    const csvContent = [headers, ...csvData]
        .map(row => row.map(field => `"${field}"`).join(','))
        .join('\n');
    
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    
    link.setAttribute('href', url);
    link.setAttribute('download', `交易记录_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = 'hidden';
    
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

// 启动应用
document.addEventListener('DOMContentLoaded', initApp);
class ReportApp {
    constructor() {
        this.auth = firebase.auth();
        this.db = firebase.database();
        this.currentUser = null;
        this.fullReportData = {}; // [新增] 初始化完整数据容器
        this.reportData = {};
        this.grid = null;
        
        this.initEventListeners();
        this.checkAuthState();
    }

    initEventListeners() {
        // 登录按钮
        document.getElementById('google-login').addEventListener('click', () => this.signInWithGoogle());
        
        // 登出按钮
        document.getElementById('logout-btn').addEventListener('click', () => this.signOut());
        
        // 导航菜单
        document.querySelectorAll('.nav-item').forEach(item => {
            item.addEventListener('click', (e) => this.switchSection(e.currentTarget.dataset.section));
        });
        
        // 报表操作
        document.getElementById('refresh-report').addEventListener('click', () => this.loadReportData());
        document.getElementById('export-csv').addEventListener('click', () => this.exportToCSV());
        
        // 筛选控件
        document.getElementById('currency-filter').addEventListener('change', () => this.applyFilters());
        document.getElementById('pl-filter').addEventListener('change', () => this.applyFilters());
        document.getElementById('search-input').addEventListener('input', () => this.applyFilters());
        
        // 导出操作
        document.getElementById('start-export').addEventListener('click', () => this.handleExport());
    }

    // 认证状态检查
    checkAuthState() {
        this.auth.onAuthStateChanged((user) => {
            if (user) {
                if (isUserAuthorized(user.email)) {
                    this.handleSignIn(user);
                } else {
                    this.showMessage('❌ 此账户未授权访问系统', 'error');
                    this.signOut();
                }
            } else {
                this.handleSignOut();
            }
        });
    }

    // Google登录
    async signInWithGoogle() {
        const provider = new firebase.auth.GoogleAuthProvider();
        provider.addScope('email');
        
        try {
            await this.auth.signInWithPopup(provider);
        } catch (error) {
            this.showMessage(`登录失败: ${error.message}`, 'error');
        }
    }

    // 登出
    async signOut() {
        try {
            await this.auth.signOut();
        } catch (error) {
            console.error('登出错误:', error);
        }
    }

    // 登录成功处理
    handleSignIn(user) {
        this.currentUser = user;
        document.getElementById('login-container').classList.add('hidden');
        document.getElementById('app-container').classList.remove('hidden');
        
        document.getElementById('user-info').textContent = `欢迎, ${user.email}`;
        this.showMessage('✅ 登录成功', 'success');
        
        // 加载报表数据
        this.loadReportData();
    }

    // 登出处理
    handleSignOut() {
        this.currentUser = null;
        document.getElementById('login-container').classList.remove('hidden');
        document.getElementById('app-container').classList.add('hidden');
        this.showMessage('已登出', '');
    }

    // 切换内容区域
    switchSection(sectionId) {
        // 更新导航状态
        document.querySelectorAll('.nav-item').forEach(item => {
            item.classList.remove('active');
        });
        document.querySelector(`[data-section="${sectionId}"]`).classList.add('active');
        
        // 更新内容显示
        document.querySelectorAll('.content-section').forEach(section => {
            section.classList.remove('active');
        });
        document.getElementById(`${sectionId}-section`).classList.add('active');
    }

    // 加载报表数据
    async loadReportData() {
        try {
            this.showMessage('正在加载报表数据...', '');
            const snapshot = await this.db.ref('reports/holdings').once('value');
            
            /*
            this.reportData = snapshot.val() || {};
            */
            // --- 修改开始 ---
            const data = snapshot.val() || {};
            this.fullReportData = data; // [新增] 保存一份永久的完整数据备份
            this.reportData = data;     // 当前用于显示的数据
            // --- 修改结束 ---

            this.initializeGrid();
            this.updateSummary();
            this.showMessage('✅ 数据加载成功', 'success');
        } catch (error) {
            this.showMessage(`加载报表数据失败: ${error.message}`, 'error');
        }
    }

    // 初始化Grid.js表格
    initializeGrid() {
        const data = this.formatDataForGrid();
        
        if (this.grid) {
            this.grid.updateConfig({
                data: data
            }).forceRender();
        } else {
            this.grid = new gridjs.Grid({
                columns: [
                    {
                        name: '股票代码',
                        width: '120px',
                        formatter: (cell) => gridjs.html(`<span class="ticker">${cell}</span>`)
                    },
                    {
                        name: '公司名称',
                        width: '200px'
                    },
                    {
                        name: '持仓数量',
                        width: '100px'
                    },
                    {
                        name: '平均成本',
                        width: '100px',
                        formatter: (cell) => `¥${cell.toFixed(2)}`
                    },
                    {
                        name: '当前价格',
                        width: '100px',
                        formatter: (cell) => `¥${cell.toFixed(2)}`
                    },
                    {
                        name: '总成本(CNY)',
                        width: '120px',
                        formatter: (cell) => `¥${cell.toFixed(2)}`
                    },
                    {
                        name: '总市值(CNY)',
                        width: '120px',
                        formatter: (cell) => `¥${cell.toFixed(2)}`
                    },
                    {
                        name: '盈亏比例',
                        width: '100px',
                        formatter: (cell) => {
                            const color = cell >= 0 ? '#10b981' : '#ef4444';
                            return gridjs.html(`<span style="color: ${color}; font-weight: bold;">${(cell).toFixed(2)}%</span>`);
                        }
                    },
                    {
                        name: '盈亏金额',
                        width: '120px',
                        formatter: (cell) => {
                            const color = cell >= 0 ? '#10b981' : '#ef4444';
                            const symbol = cell >= 0 ? '+' : '';
                            return gridjs.html(`<span style="color: ${color}; font-weight: bold;">${symbol}¥${cell.toFixed(2)}</span>`);
                        }
                    },
                    {
                        name: '货币',
                        width: '80px'
                    },
                    {
                        name: '更新时间',
                        width: '180px',
                        formatter: (cell) => new Date(cell).toLocaleString('zh-CN')
                    }
                ],
                data: data,
                pagination: {
                    limit: 20,
                    summary: true
                },
                search: true,
                sort: true,
                resizable: true,
                style: {
                    table: {
                        'font-size': '14px'
                    }
                }
            }).render(document.getElementById('report-grid-wrapper'));
        }
    }

    // 格式化数据供Grid.js使用
    formatDataForGrid() {
        return Object.entries(this.reportData).map(([key, holding]) => [
            holding.ticker || key,
            holding.company || 'N/A',
            holding.totalHolding || 0,
            holding.avgCostPrice || 0,
            holding.currentPrice || 0,
            holding.costCNY || 0,
            holding.valueCNY || 0,
            holding.PLRatio || 0,
            (holding.valueCNY || 0) - (holding.costCNY || 0),
            holding.currency || 'N/A',
            holding.calculatedAt || 'N/A'
        ]);
    }

    // 应用筛选条件
    applyFilters() {
        const currencyFilter = document.getElementById('currency-filter').value;
        const plFilter = document.getElementById('pl-filter').value;
        const searchText = document.getElementById('search-input').value.toLowerCase();

        // --- 修改开始 ---
        // 注意：这里改为从 this.fullReportData 进行 filter
        // 如果 this.fullReportData 为空(尚未加载)，则使用空对象
        const sourceData = this.fullReportData || {};

        //const filteredData = Object.entries(this.reportData).filter(([key, holding]) => {
        const filteredData = Object.entries(sourceData).filter(([key, holding]) => {
            // 货币筛选
            if (currencyFilter && holding.currency !== currencyFilter) {
                return false;
            }
            
            // 盈亏筛选
            if (plFilter === 'profit' && (holding.PLRatio || 0) < 0) {
                return false;
            }
            if (plFilter === 'loss' && (holding.PLRatio || 0) >= 0) {
                return false;
            }
            
            // 搜索筛选
            if (searchText) {
                const tickerMatch = (holding.ticker || '').toLowerCase().includes(searchText);
                const companyMatch = (holding.company || '').toLowerCase().includes(searchText);
                if (!tickerMatch && !companyMatch) {
                    return false;
                }
            }
            
            return true;
        });

        const filteredObject = Object.fromEntries(filteredData);
        this.reportData = filteredObject;
        this.initializeGrid();
        this.updateSummary();
    }

    // 更新统计摘要
    updateSummary() {
        const holdings = Object.values(this.reportData);
        
        const totalHoldings = holdings.length;
        const totalCost = holdings.reduce((sum, h) => sum + (h.costCNY || 0), 0);
        const totalValue = holdings.reduce((sum, h) => sum + (h.valueCNY || 0), 0);
        const totalPL = totalValue - totalCost;

        document.getElementById('total-holdings').textContent = totalHoldings;
        document.getElementById('total-cost').textContent = `¥${totalCost.toFixed(2)}`;
        document.getElementById('total-value').textContent = `¥${totalValue.toFixed(2)}`;
        
        const plElement = document.getElementById('total-pl');
        plElement.textContent = `¥${totalPL.toFixed(2)}`;
        plElement.style.color = totalPL >= 0 ? '#10b981' : '#ef4444';
    }

    // 导出为CSV
    exportToCSV() {
        const headers = ['股票代码', '公司名称', '持仓数量', '平均成本', '当前价格', '总成本(CNY)', '总市值(CNY)', '盈亏比例', '盈亏金额', '货币', '更新时间'];
        const data = this.formatDataForGrid();
        
        let csvContent = headers.join(',') + '\n';
        data.forEach(row => {
            const formattedRow = row.map(cell => {
                if (typeof cell === 'number') {
                    return cell.toString();
                }
                return `"${cell}"`;
            });
            csvContent += formattedRow.join(',') + '\n';
        });

        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', `持仓报表_${new Date().toISOString().split('T')[0]}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }

    // 处理导出
    handleExport() {
        const format = document.getElementById('export-format').value;
        const range = document.getElementById('export-range').value;
        
        if (format === 'csv') {
            this.exportToCSV();
        } else {
            this.showMessage('该导出格式开发中...', '');
        }
    }

    // 显示消息
    showMessage(message, type) {
        const messageEl = document.getElementById('login-message');
        messageEl.textContent = message;
        messageEl.className = `message ${type}`;
        
        if (message) {
            setTimeout(() => {
                messageEl.textContent = '';
                messageEl.className = 'message';
            }, 5000);
        }
    }
}

// 初始化应用
const reportApp = new ReportApp();
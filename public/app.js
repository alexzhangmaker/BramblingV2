class FirebaseAdminApp {
    constructor() {
        this.auth = firebase.auth();
        this.db = firebase.database();
        this.currentUser = null;
        this.accountsData = {};
        
        this.assetsData = {}; // 存储证券资产数据
        this.dealsData = {};  // 存储交易记录数据
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
        
        // 账户管理操作
        document.getElementById('refresh-accounts').addEventListener('click', () => this.loadAccounts());
        document.getElementById('add-account').addEventListener('click', () => this.showAddAccountModal());
        
        // 模态框操作
        document.getElementById('modal-close').addEventListener('click', () => this.hideModal());
        document.getElementById('modal-cancel').addEventListener('click', () => this.hideModal());
        document.getElementById('modal-save').addEventListener('click', () => this.saveAccount());
        
        // 持仓管理
        document.getElementById('account-select').addEventListener('change', (e) => this.loadHoldings(e.target.value));
        document.getElementById('refresh-holdings').addEventListener('click', () => {
            const selectedAccount = document.getElementById('account-select').value;
            if (selectedAccount) this.loadHoldings(selectedAccount);
        });

        // 交易管理操作
        document.getElementById('refresh-deals').addEventListener('click', () => this.loadDeals());
        document.getElementById('add-deal').addEventListener('click', () => this.showAddDealModal());
        
        // 交易模态框操作
        document.getElementById('deal-modal-close').addEventListener('click', () => this.hideDealModal());
        document.getElementById('deal-modal-cancel').addEventListener('click', () => this.hideDealModal());
        document.getElementById('deal-modal-save').addEventListener('click', () => this.saveDeal());
        
        // 证券代码输入自动完成
        document.getElementById('deal-ticker').addEventListener('input', (e) => this.handleTickerInput(e.target.value));
        document.getElementById('deal-ticker').addEventListener('change', (e) => this.handleTickerChange(e.target.value));

        // 在 initEventListeners 方法中添加
        document.getElementById('add-holding').addEventListener('click', () => this.showAddHolding());
    }


    // 添加持仓管理相关方法
showAddHolding() {
    const selectedAccount = document.getElementById('account-select').value;
    if (!selectedAccount) {
        this.showMessage('请先选择账户', 'error');
        return;
    }
    
    this.addNewHoldingRow(selectedAccount);
}

addNewHoldingRow(accountId) {
    const holdingsContent = document.getElementById('holdings-content');
    
    // 确保 holdingsContent 包含表格结构
    if (!holdingsContent.querySelector('table')) {
        this.loadHoldings(accountId);
        // 等待表格加载完成
        setTimeout(() => {
            this.addNewHoldingRow(accountId);
        }, 100);
        return;
    }
    
    const tbody = holdingsContent.querySelector('tbody');
    const newRow = document.createElement('tr');
    newRow.className = 'new-holding-row';
    newRow.innerHTML = `
        <td>
            <input type="text" class="editable-input ticker-input" placeholder="证券代码" required>
        </td>
        <td>
            <input type="text" class="editable-input" placeholder="公司名称">
        </td>
        <td>
            <input type="number" class="editable-input holding-quantity" min="0" step="1" value="0" required>
        </td>
        <td>
            <input type="number" class="editable-input cost-input" min="0" step="0.001" value="0" required>
        </td>
        <td>
            <select class="editable-input currency-select">
                <option value="CNY">CNY</option>
                <option value="USD">USD</option>
                <option value="HKD">HKD</option>
            </select>
        </td>
        <td>
            <select class="editable-input exchange-select">
                <option value="SS">上海</option>
                <option value="SZ">深圳</option>
                <option value="HK">香港</option>
                <option value="US">美国</option>
            </select>
        </td>
        <td>
            <button class="btn btn-success save-new-holding">保存</button>
            <button class="btn btn-secondary cancel-new-holding">取消</button>
        </td>
    `;
    
    tbody.insertBefore(newRow, tbody.firstChild);
    
    // 添加事件监听
    newRow.querySelector('.save-new-holding').addEventListener('click', () => this.saveNewHolding(accountId, newRow));
    newRow.querySelector('.cancel-new-holding').addEventListener('click', () => newRow.remove());
    
    // 添加回车保存功能
    newRow.querySelectorAll('.editable-input').forEach(input => {
        input.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                this.saveNewHolding(accountId, newRow);
            }
        });
    });
}

async saveNewHolding(accountId, row) {
    const ticker = row.querySelector('.ticker-input').value.trim();
    const company = row.querySelector('.editable-input:nth-child(2)').value.trim();
    const quantity = parseInt(row.querySelector('.holding-quantity').value);
    const costPerShare = parseFloat(row.querySelector('.cost-input').value);
    const currency = row.querySelector('.currency-select').value;
    const exchange = row.querySelector('.exchange-select').value;
    
    // 数据验证
    if (!ticker) {
        this.showMessage('请输入证券代码', 'error');
        return;
    }
    
    if (quantity < 0) {
        this.showMessage('持仓数量不能为负数', 'error');
        return;
    }
    
    if (costPerShare < 0) {
        this.showMessage('成本价不能为负数', 'error');
        return;
    }
    
    try {
        const holdingKey = this.generateHoldingKey(ticker, exchange);
        const holdingData = {
            ticker: ticker,
            company: company,
            holding: quantity,
            costPerShare: parseFloat(costPerShare.toFixed(3)),
            currency: currency,
            exchange: exchange,
            exchangeCode: exchange,
            assetClass: 'STK',
            description: company,
            lastUpdated: new Date().toISOString(),
            createdBy: this.currentUser.email
        };
        
        await this.db.ref(`accounts/${accountId}/holdings/${holdingKey}`).set(holdingData);
        this.showMessage('✅ 持仓添加成功', 'success');
        this.loadHoldings(accountId); // 重新加载持仓数据
    } catch (error) {
        this.showMessage(`保存失败: ${error.message}`, 'error');
    }
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
        
        // 加载数据
        this.loadAccounts();
        this.loadAssets(); // 加载证券资产数据
        this.populateAccountSelect();
    }

    // 登出处理
    handleSignOut() {
        this.currentUser = null;
        document.getElementById('login-container').classList.remove('hidden');
        document.getElementById('app-container').classList.add('hidden');
        this.showMessage('已登出', '');
    }

    // 加载证券资产数据
    async loadAssets() {
        try {
            const snapshot = await this.db.ref('assets').once('value');
            this.assetsData = snapshot.val() || {};
            this.populateTickerDatalist();
        } catch (error) {
            console.error('加载证券资产数据失败:', error);
        }
    }

    // 填充证券代码下拉列表
    populateTickerDatalist() {
        const datalist = document.getElementById('ticker-list');
        datalist.innerHTML = '';
        
        Object.values(this.assetsData).forEach(asset => {
            const option = document.createElement('option');
            option.value = asset.ticker;
            option.textContent = `${asset.ticker} - ${asset.company}`;
            datalist.appendChild(option);
        });
    }

    // 处理证券代码输入
    handleTickerInput(ticker) {
        // 实时验证和自动填充
        if (ticker.length > 2) {
            this.handleTickerChange(ticker);
        }
    }

    // 处理证券代码选择
    handleTickerChange(ticker) {
        // 查找匹配的资产
        const asset = Object.values(this.assetsData).find(a => a.ticker === ticker);
        
        if (asset) {
            // 自动填充相关信息
            document.getElementById('deal-company').value = asset.company || '';
            document.getElementById('deal-currency').value = asset.currency || '';
            document.getElementById('deal-exchange').value = asset.exchange || '';
        } else {
            // 清空自动填充的字段
            document.getElementById('deal-company').value = '';
            document.getElementById('deal-currency').value = '';
            document.getElementById('deal-exchange').value = '';
        }
    }

    // 加载交易记录
    async loadDeals() {
        try {
            const snapshot = await this.db.ref('dealLogs').once('value');
            this.dealsData = snapshot.val() || {};
            this.displayDeals(this.dealsData);
        } catch (error) {
            this.showMessage(`加载交易记录失败: ${error.message}`, 'error');
        }
    }

    /*
    // 显示交易记录表格
    displayDeals(deals) {
        const tbody = document.getElementById('deals-tbody');
        tbody.innerHTML = '';

        if (!deals || Object.keys(deals).length === 0) {
            tbody.innerHTML = '<tr><td colspan="10" style="text-align: center;">暂无交易记录</td></tr>';
            return;
        }

        // 按交易日期倒序排列
        const sortedDeals = Object.entries(deals).sort(([keyA], [keyB]) => keyB.localeCompare(keyA));

        sortedDeals.forEach(([dealId, dealData]) => {
            const row = document.createElement('tr');
            const operationText = dealData.operation === 'BUY' ? '买入' : '卖出';
            const operationClass = dealData.operation === 'BUY' ? 'deal-buy' : 'deal-sell';
            
            row.innerHTML = `
                <td>${this.formatDate(dealData.date)}</td>
                <td><span class="deal-operation ${operationClass}">${operationText}</span></td>
                <td><strong>${dealData.ticker}</strong></td>
                <td>${dealData.company || 'N/A'}</td>
                <td>${dealData.quantity}</td>
                <td>${dealData.price ? dealData.price.toFixed(3) : 'N/A'}</td>
                <td>${dealData.currency || 'N/A'}</td>
                <td>${dealData.accountID || 'N/A'}</td>
                <td>${dealData.exchange || 'N/A'}</td>
                <td>
                    <button class="btn btn-primary" onclick="app.editDeal('${dealId}')">编辑</button>
                    <button class="btn btn-danger" onclick="app.deleteDeal('${dealId}')">删除</button>
                </td>
            `;
            tbody.appendChild(row);
        });
    }
    */
    // 显示交易记录表格 - 修改部分
    displayDeals(deals) {
        const tbody = document.getElementById('deals-tbody');
        tbody.innerHTML = '';

        if (!deals || Object.keys(deals).length === 0) {
            tbody.innerHTML = '<tr><td colspan="10" style="text-align: center;">暂无交易记录</td></tr>';
            return;
        }

        // 按交易日期倒序排列
        const sortedDeals = Object.entries(deals).sort(([keyA], [keyB]) => keyB.localeCompare(keyA));

        sortedDeals.forEach(([dealId, dealData]) => {
            const row = document.createElement('tr');
            const operationText = dealData.operation === 'BUY' ? '买入' : '卖出';
            const operationClass = dealData.operation === 'BUY' ? 'deal-buy' : 'deal-sell';
            const statusText = dealData.status === 'Committed' ? '已提交' : '待提交';
            const statusClass = dealData.status === 'Committed' ? 'deal-committed' : 'deal-tocommit';
            
            row.innerHTML = `
                <td>${this.formatDate(dealData.date)}</td>
                <td><span class="deal-operation ${operationClass}">${operationText}</span></td>
                <td><strong>${dealData.ticker}</strong></td>
                <td>${dealData.company || 'N/A'}</td>
                <td>${dealData.quantity}</td>
                <td>${dealData.price ? dealData.price.toFixed(3) : 'N/A'}</td>
                <td>${dealData.currency || 'N/A'}</td>
                <td>${dealData.accountID || 'N/A'}</td>
                <td><span class="deal-status ${statusClass}">${statusText}</span></td>
                <td>
                    <button class="btn btn-primary" onclick="app.editDeal('${dealId}')">编辑</button>
                    <button class="btn btn-danger" onclick="app.deleteDeal('${dealId}')">删除</button>
                    ${dealData.status !== 'Committed' ? 
                        `<button class="btn btn-success" onclick="app.onCommitDeal('${dealId}')">提交</button>` : 
                        ''
                    }
                </td>
            `;
            tbody.appendChild(row);
        });
    }

    // 提交交易订单
    async onCommitDeal(dealId) {
        if (!confirm(`确定要提交交易 ${dealId} 吗？提交后将更新账户持仓和资金，此操作不可撤销。`)) {
            return;
        }

        try {
            // 获取交易数据
            const dealSnapshot = await this.db.ref(`dealLogs/${dealId}`).once('value');
            const dealData = dealSnapshot.val();
            
            if (!dealData) {
                this.showMessage('交易记录不存在', 'error');
                return;
            }

            if (dealData.status === 'Committed') {
                this.showMessage('该交易已提交，无需重复提交', 'warning');
                return;
            }

            // 获取账户数据
            const accountSnapshot = await this.db.ref(`accounts/${dealData.accountID}`).once('value');
            const accountData = accountSnapshot.val();
            
            if (!accountData) {
                this.showMessage('账户不存在', 'error');
                return;
            }

            // 执行提交逻辑
            await this.executeDealCommit(dealId, dealData, accountData);
            
            this.showMessage('✅ 交易提交成功', 'success');
            this.loadDeals(); // 刷新交易列表
            this.loadAccounts(); // 刷新账户数据

        } catch (error) {
            this.showMessage(`提交失败: ${error.message}`, 'error');
            console.error('提交交易错误:', error);
        }
    }

    /*
    // 执行交易提交逻辑
    async executeDealCommit(dealId, dealData, accountData) {
        const transactionTotal = dealData.quantity * dealData.price;
        const currency = dealData.currency || 'CNY';
        
        // 开始事务处理
        const updates = {};

        // 1. 更新交易状态
        updates[`dealLogs/${dealId}/status`] = 'Committed';
        updates[`dealLogs/${dealId}/committedAt`] = new Date().toISOString();
        updates[`dealLogs/${dealId}/committedBy`] = this.currentUser.email;

        // 2. 更新持仓数据
        const holdingKey = this.generateHoldingKey(dealData.ticker, dealData.exchange);
        const currentHoldings = accountData.holdings || {};
        const currentHolding = currentHoldings[holdingKey] || {
            ticker: dealData.ticker,
            company: dealData.company,
            currency: dealData.currency,
            exchange: dealData.exchange,
            holding: 0,
            costPerShare: 0,
            totalCost: 0
        };

        let newHolding;
        if (dealData.operation === 'BUY') {
            newHolding = this.calculateBuyHolding(currentHolding, dealData.quantity, dealData.price);
        } else {
            newHolding = this.calculateSellHolding(currentHolding, dealData.quantity, dealData.price);
        }

        updates[`accounts/${dealData.accountID}/holdings/${holdingKey}`] = newHolding;

        // 3. 更新现金或债务数据
        const cashChanges = await this.calculateCashChanges(accountData, dealData, transactionTotal, currency);
        
        if (cashChanges.cash) {
            updates[`accounts/${dealData.accountID}/cash/${currency}`] = cashChanges.cash;
        }
        
        if (cashChanges.debt !== undefined) {
            updates[`accounts/${dealData.accountID}/debt/${currency}`] = cashChanges.debt;
        }

        // 4. 更新账户元数据
        updates[`accounts/${dealData.accountID}/meta/lastUpdated`] = new Date().toISOString();
        updates[`accounts/${dealData.accountID}/meta/updatedBy`] = this.currentUser.email;

        // 执行批量更新
        await this.db.ref().update(updates);
    }
    */
    /*
    // 执行交易提交逻辑 - 修正持仓匹配
    async executeDealCommit(dealId, dealData, accountData) {
        const transactionTotal = dealData.quantity * dealData.price;
        const currency = dealData.currency || 'CNY';
        
        // 开始事务处理
        const updates = {};

        // 1. 更新交易状态
        updates[`dealLogs/${dealId}/status`] = 'Committed';
        updates[`dealLogs/${dealId}/committedAt`] = new Date().toISOString();
        updates[`dealLogs/${dealId}/committedBy`] = this.currentUser.email;

        // 2. 更新持仓数据 - 修正：通过ticker查找现有持仓
        const currentHoldings = accountData.holdings || {};
        
        // 查找匹配的持仓记录（通过ticker匹配）
        const existingHoldingKey = this.findHoldingByTicker(currentHoldings, dealData.ticker);
        let currentHolding;

        if (existingHoldingKey) {
            // 找到现有持仓
            currentHolding = currentHoldings[existingHoldingKey];
        } else {
            // 新建持仓记录
            currentHolding = {
                ticker: dealData.ticker,
                company: dealData.company,
                currency: dealData.currency,
                exchange: dealData.exchange,
                exchangeCode: dealData.exchange,
                assetClass: 'STK', // 默认值
                description: dealData.company,
                holding: 0,
                costPerShare: 0,
                totalCost: 0
            };
        }

        let newHolding;
        if (dealData.operation === 'BUY') {
            newHolding = this.calculateBuyHolding(currentHolding, dealData.quantity, dealData.price);
        } else {
            newHolding = this.calculateSellHolding(currentHolding, dealData.quantity, dealData.price);
        }

        // 确定持仓键
        const holdingKey = existingHoldingKey || this.generateHoldingKey(dealData.ticker, dealData.exchange);
        updates[`accounts/${dealData.accountID}/holdings/${holdingKey}`] = newHolding;

        // 3. 更新现金或债务数据
        const cashChanges = await this.calculateCashChanges(accountData, dealData, transactionTotal, currency);
        
        if (cashChanges.cash !== undefined) {
            updates[`accounts/${dealData.accountID}/cash/${currency}`] = cashChanges.cash;
        }
        
        if (cashChanges.debt !== undefined) {
            updates[`accounts/${dealData.accountID}/debt/${currency}`] = cashChanges.debt;
        }

        // 4. 更新账户元数据
        updates[`accounts/${dealData.accountID}/meta/lastUpdated`] = new Date().toISOString();
        updates[`accounts/${dealData.accountID}/meta/updatedBy`] = this.currentUser.email;

        // 执行批量更新
        await this.db.ref().update(updates);
    }
    */
    // 执行交易提交逻辑 - 添加调试信息
    async executeDealCommit(dealId, dealData, accountData) {
        const transactionTotal = dealData.quantity * dealData.price;
        const currency = dealData.currency || 'CNY';
        
        console.log('开始执行交易提交:', {
            dealId,
            dealData,
            accountHoldings: accountData.holdings,
            transactionTotal,
            currency
        });

        // 开始事务处理
        const updates = {};

        // 1. 更新交易状态
        updates[`dealLogs/${dealId}/status`] = 'Committed';
        updates[`dealLogs/${dealId}/committedAt`] = new Date().toISOString();
        updates[`dealLogs/${dealId}/committedBy`] = this.currentUser.email;

        // 2. 更新持仓数据
        const currentHoldings = accountData.holdings || {};
        
        // 查找匹配的持仓记录（通过ticker匹配）
        const existingHoldingKey = this.findHoldingByTicker(currentHoldings, dealData.ticker);
        let currentHolding;

        if (existingHoldingKey) {
            // 找到现有持仓
            currentHolding = currentHoldings[existingHoldingKey];
            console.log('找到现有持仓:', { existingHoldingKey, currentHolding });
        } else {
            // 新建持仓记录
            currentHolding = {
                ticker: dealData.ticker,
                company: dealData.company,
                currency: dealData.currency,
                exchange: dealData.exchange,
                exchangeCode: dealData.exchange,
                assetClass: 'STK', // 默认值
                description: dealData.company,
                holding: 0,
                costPerShare: 0
            };
            console.log('创建新持仓:', { currentHolding });
        }

        let newHolding;
        if (dealData.operation === 'BUY') {
            newHolding = this.calculateBuyHolding(currentHolding, dealData.quantity, dealData.price);
        } else {
            newHolding = this.calculateSellHolding(currentHolding, dealData.quantity, dealData.price);
        }

        console.log('计算后的新持仓:', { newHolding });

        // 确定持仓键
        const holdingKey = existingHoldingKey || this.generateHoldingKey(dealData.ticker, dealData.exchange);
        updates[`accounts/${dealData.accountID}/holdings/${holdingKey}`] = newHolding;

        // 3. 更新现金或债务数据
        const cashChanges = await this.calculateCashChanges(accountData, dealData, transactionTotal, currency);
        
        if (cashChanges.cash !== undefined) {
            updates[`accounts/${dealData.accountID}/cash/${currency}`] = cashChanges.cash;
        }
        
        if (cashChanges.debt !== undefined) {
            updates[`accounts/${dealData.accountID}/debt/${currency}`] = cashChanges.debt;
        }

        // 4. 更新账户元数据
        updates[`accounts/${dealData.accountID}/meta/lastUpdated`] = new Date().toISOString();
        updates[`accounts/${dealData.accountID}/meta/updatedBy`] = this.currentUser.email;

        console.log('准备执行的更新操作:', updates);

        // 执行批量更新
        await this.db.ref().update(updates);
        
        console.log('交易提交完成');
    }


    // 通过ticker查找现有的持仓记录
    findHoldingByTicker(holdings, ticker) {
        for (const [key, holding] of Object.entries(holdings)) {
            if (holding.ticker === ticker) {
                return key; // 返回找到的持仓键
            }
        }
        return null; // 没有找到匹配的持仓
    }

    /*
    // 生成持仓键
    generateHoldingKey(ticker, exchange) {
        // 简单的键生成逻辑，可以根据需要调整
        return `${ticker}_${exchange}`.replace(/[.#$[\]]/g, '_');
    }
    */
    // 生成持仓键 - 优化逻辑以匹配现有格式
    generateHoldingKey(ticker, exchange) {
        // 根据你提供的持仓数据结构，键格式为：ticker中的数字部分 + "_" + exchange
        // 例如: "0014.HK" + "HK" -> "0014_HK"
        // "600007.SS" + "CN" -> "600007_SS"
        
        // 提取ticker的数字部分
        const tickerNumber = ticker.split('.')[0];
        
        // 根据exchange确定后缀
        let exchangeSuffix;
        switch (exchange) {
            case 'HK':
                exchangeSuffix = 'HK';
                break;
            case 'CN':
            case 'SS':
                exchangeSuffix = 'SS';
                break;
            case 'US':
                exchangeSuffix = 'US';
                break;
            default:
                exchangeSuffix = exchange;
        }
        
        return `${tickerNumber}_${exchangeSuffix}`;
    }

    /*
    // 计算买入后的持仓
    calculateBuyHolding(currentHolding, quantity, price) {
        const totalShares = currentHolding.holding + quantity;
        const totalCost = currentHolding.totalCost + (quantity * price);
        const avgCost = totalCost / totalShares;

        return {
            ...currentHolding,
            holding: totalShares,
            costPerShare: avgCost,
            totalCost: totalCost,
            lastUpdated: new Date().toISOString()
        };
    }
    */
    
    
    // 计算买入后的持仓 - 修正成本计算逻辑
    calculateBuyHolding(currentHolding, quantity, price) {
        const currentShares = currentHolding.holding || 0;
        const currentCostPerShare = currentHolding.costPerShare || 0;
        
        // 计算当前持仓的总成本
        const currentTotalCost = currentShares * currentCostPerShare;
        // 计算新买入的总成本
        const newTotalCost = quantity * price;
        // 计算合并后的总成本
        const totalCost = currentTotalCost + newTotalCost;
        // 计算合并后的总股数
        const totalShares = currentShares + quantity;
        // 计算新的平均成本
        const avgCost = totalShares > 0 ? totalCost / totalShares : 0;

        console.log('买入成本计算详情:', {
            currentShares,
            currentCostPerShare,
            currentTotalCost,
            quantity,
            price,
            newTotalCost,
            totalCost,
            totalShares,
            avgCost
        });

        return {
            ...currentHolding,
            holding: totalShares,
            costPerShare: parseFloat(avgCost.toFixed(3)), // 保留3位小数
            lastUpdated: new Date().toISOString()
            // 注意：移除 totalCost 字段，因为你的数据结构中没有这个字段
        };
    }

    
    // 计算卖出后的持仓 - 修正成本计算逻辑
    calculateSellHolding(currentHolding, quantity, price) {
        const currentHoldingQty = currentHolding.holding || 0;
        const currentCostPerShare = currentHolding.costPerShare || 0;
        
        if (currentHoldingQty < quantity) {
            throw new Error(`持仓数量不足: 当前持仓 ${currentHoldingQty}，卖出数量 ${quantity}`);
        }

        const totalShares = currentHoldingQty - quantity;
        
        // 卖出时成本价保持不变（先进先出或平均成本法）
        // 因为卖出的部分按原始成本计算，剩余持仓的成本价不变
        const avgCost = currentCostPerShare;

        console.log('卖出成本计算详情:', {
            currentHoldingQty,
            currentCostPerShare,
            quantity,
            totalShares,
            avgCost
        });

        return {
            ...currentHolding,
            holding: totalShares,
            costPerShare: parseFloat(avgCost.toFixed(3)),
            lastUpdated: new Date().toISOString()
        };
    }

    /*
    // 计算卖出后的持仓
    calculateSellHolding(currentHolding, quantity, price) {
        if (currentHolding.holding < quantity) {
            throw new Error(`持仓数量不足: 当前持仓 ${currentHolding.holding}，卖出数量 ${quantity}`);
        }

        const totalShares = currentHolding.holding - quantity;
        
        // 卖出时成本价保持不变，只减少持仓数量
        const soldCost = quantity * currentHolding.costPerShare;
        const totalCost = currentHolding.totalCost - soldCost;

        return {
            ...currentHolding,
            holding: totalShares,
            totalCost: totalCost,
            lastUpdated: new Date().toISOString()
        };
    }
    */

    /*
    // 计算现金变化
    async calculateCashChanges(accountData, dealData, transactionTotal, currency) {
        const currentCash = accountData.cash?.[currency] || 0;
        const currentDebt = accountData.debt?.[currency] || 0;
        
        const changes = {};

        if (dealData.operation === 'BUY') {
            // 买入：减少现金或增加债务
            if (currentCash >= transactionTotal) {
                // 现金充足
                changes.cash = currentCash - transactionTotal;
                changes.debt = currentDebt; // 债务不变
            } else {
                // 现金不足，需要借钱
                const shortage = transactionTotal - currentCash;
                changes.cash = 0;
                changes.debt = currentDebt + shortage;
            }
        } else {
            // 卖出：增加现金或减少债务
            changes.cash = currentCash + transactionTotal;
            
            // 如果有债务，优先偿还债务
            if (currentDebt > 0) {
                const debtRepayment = Math.min(transactionTotal, currentDebt);
                changes.cash = currentCash + (transactionTotal - debtRepayment);
                changes.debt = currentDebt - debtRepayment;
            } else {
                changes.debt = currentDebt; // 债务不变
            }
        }

        return changes;
    }
    */
    // 计算现金变化 - 修正逻辑
    async calculateCashChanges(accountData, dealData, transactionTotal, currency) {
        const currentCash = accountData.cash?.[currency] || 0;
        const currentDebt = accountData.debt?.[currency] || 0;
        
        const changes = {};

        if (dealData.operation === 'BUY') {
            // 买入：减少现金或增加债务
            if (currentCash >= transactionTotal) {
                // 现金充足
                changes.cash = currentCash - transactionTotal;
                changes.debt = currentDebt; // 债务不变
            } else {
                // 现金不足，需要借钱
                const shortage = transactionTotal - currentCash;
                changes.cash = 0;
                changes.debt = currentDebt + shortage;
            }
        } else {
            // 卖出：增加现金
            changes.cash = currentCash + transactionTotal;
            
            // 如果有债务，优先偿还债务
            if (currentDebt > 0) {
                const debtRepayment = Math.min(transactionTotal, currentDebt);
                changes.cash = currentCash + (transactionTotal - debtRepayment);
                changes.debt = currentDebt - debtRepayment;
            } else {
                changes.debt = currentDebt; // 债务不变
            }
        }

        // 确保数值精度
        changes.cash = parseFloat(changes.cash.toFixed(2));
        if (changes.debt !== undefined) {
            changes.debt = parseFloat(changes.debt.toFixed(2));
        }

        return changes;
    }

    // 格式化日期
    formatDate(dateString) {
        if (!dateString) return 'N/A';
        const date = new Date(dateString);
        return date.toLocaleDateString('zh-CN');
    }

    // 显示新增交易模态框
    showAddDealModal() {
        document.getElementById('deal-modal-title').textContent = '新增交易记录';
        document.getElementById('deal-form').reset();
        document.getElementById('edit-deal-id').value = '';
        
        // 设置默认日期为今天
        const today = new Date().toISOString().split('T')[0];
        document.getElementById('deal-date').value = today;
        
        // 填充账户下拉框
        this.populateDealAccountSelect();
        
        this.showDealModal();
    }

    // 编辑交易记录
    editDeal(dealId) {
        const dealData = this.dealsData[dealId];
        if (!dealData) return;

        if (dealData.status === 'Committed') {
            this.showMessage('已提交的交易不可编辑', 'warning');
            return;
        }

        document.getElementById('deal-modal-title').textContent = `编辑交易记录 ${dealId}`;
        document.getElementById('edit-deal-id').value = dealId;
        
        // 填充表单数据
        document.getElementById('deal-date').value = dealData.date || '';
        document.getElementById('deal-operation').value = dealData.operation || '';
        document.getElementById('deal-ticker').value = dealData.ticker || '';
        document.getElementById('deal-company').value = dealData.company || '';
        document.getElementById('deal-quantity').value = dealData.quantity || '';
        document.getElementById('deal-price').value = dealData.price || '';
        document.getElementById('deal-currency').value = dealData.currency || '';
        document.getElementById('deal-exchange').value = dealData.exchange || '';
        document.getElementById('deal-notes').value = dealData.notes || '';
        
        // 填充账户下拉框并选中当前值
        this.populateDealAccountSelect(dealData.accountID);
        
        this.showDealModal();
    }

    // 删除交易记录 - 修改部分（禁止删除已提交的交易）
    async deleteDeal(dealId) {
        const dealData = this.dealsData[dealId];
        
        if (dealData && dealData.status === 'Committed') {
            this.showMessage('已提交的交易不可删除', 'warning');
            return;
        }

        if (!confirm(`确定要删除交易记录 ${dealId} 吗？此操作不可撤销。`)) {
            return;
        }

        try {
            await this.db.ref(`dealLogs/${dealId}`).remove();
            this.showMessage('✅ 交易记录删除成功', 'success');
            this.loadDeals();
        } catch (error) {
            this.showMessage(`删除失败: ${error.message}`, 'error');
        }
    }

    // 填充交易账户选择下拉框
    populateDealAccountSelect(selectedAccount = '') {
        const select = document.getElementById('deal-account');
        select.innerHTML = '<option value="">选择账户...</option>';
        
        Object.keys(this.accountsData).forEach(accountId => {
            const option = document.createElement('option');
            option.value = accountId;
            option.textContent = accountId;
            option.selected = (accountId === selectedAccount);
            select.appendChild(option);
        });
    }

    // 保存交易记录
    async saveDeal() {
        const dealId = document.getElementById('edit-deal-id').value;
        const date = document.getElementById('deal-date').value;
        const operation = document.getElementById('deal-operation').value;
        const ticker = document.getElementById('deal-ticker').value;
        const company = document.getElementById('deal-company').value;
        const quantity = parseInt(document.getElementById('deal-quantity').value);
        const price = parseFloat(document.getElementById('deal-price').value);
        const currency = document.getElementById('deal-currency').value;
        const accountID = document.getElementById('deal-account').value;
        const exchange = document.getElementById('deal-exchange').value;
        const notes = document.getElementById('deal-notes').value;

        // 验证必填字段
        if (!date || !operation || !ticker || !quantity || !price || !accountID) {
            this.showMessage('请填写所有必填字段', 'error');
            return;
        }

        // 验证证券代码是否存在
        const assetExists = Object.values(this.assetsData).some(asset => asset.ticker === ticker);
        if (!assetExists) {
            this.showMessage('证券代码不存在，请选择有效的证券代码', 'error');
            return;
        }

        try {
            const dealData = {
                date: date,
                operation: operation,
                ticker: ticker,
                company: company,
                quantity: quantity,
                price: price,
                currency: currency,
                accountID: accountID,
                exchange: exchange,
                notes: notes,
                status: 'toCommit', // 新增状态字段，默认为待提交
                createdBy: this.currentUser.email,
                createdAt: new Date().toISOString(),
                updatedAt: new Date().toISOString()
            };

            let finalDealId = dealId;
            
            if (!finalDealId) {
                // 生成新的交易ID: YYYYMMDD_4digit unique serial Number
                const datePart = date.replace(/-/g, '');
                const serialNumber = await this.generateSerialNumber(datePart);
                finalDealId = `${datePart}_${serialNumber.toString().padStart(4, '0')}`;
            }

            await this.db.ref(`dealLogs/${finalDealId}`).set(dealData);
            this.hideDealModal();
            this.showMessage('✅ 交易记录保存成功', 'success');
            this.loadDeals();
        } catch (error) {
            this.showMessage(`保存失败: ${error.message}`, 'error');
        }
    }

    // 生成序列号
    async generateSerialNumber(datePart) {
        try {
            const snapshot = await this.db.ref('dealLogs')
                .orderByKey()
                .startAt(datePart)
                .endAt(datePart + '\uf8ff')
                .once('value');
            
            const deals = snapshot.val() || {};
            const existingSerials = Object.keys(deals).map(key => {
                const parts = key.split('_');
                return parts.length > 1 ? parseInt(parts[1]) : 0;
            });
            
            const maxSerial = existingSerials.length > 0 ? Math.max(...existingSerials) : 0;
            return maxSerial + 1;
        } catch (error) {
            console.error('生成序列号失败:', error);
            return 1;
        }
    }

    /*
    // 删除交易记录
    async deleteDeal(dealId) {
        if (!confirm(`确定要删除交易记录 ${dealId} 吗？此操作不可撤销。`)) {
            return;
        }

        try {
            await this.db.ref(`dealLogs/${dealId}`).remove();
            this.showMessage('✅ 交易记录删除成功', 'success');
            this.loadDeals();
        } catch (error) {
            this.showMessage(`删除失败: ${error.message}`, 'error');
        }
    }
    */

    // 显示交易模态框
    showDealModal() {
        document.getElementById('deal-modal').classList.remove('hidden');
    }

    // 隐藏交易模态框
    hideDealModal() {
        document.getElementById('deal-modal').classList.add('hidden');
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
        
        // 加载对应数据
        if (sectionId === 'accounts') {
            this.loadAccounts();
        } else if (sectionId === 'holdings') {
            this.populateAccountSelect();
        } else if (sectionId === 'deals') {
            this.loadDeals();
            this.loadAssets(); // 确保资产数据已加载
        }
    }

    // 加载账户数据
    async loadAccounts() {
        try {
            const snapshot = await this.db.ref('accounts').once('value');
            this.accountsData = snapshot.val() || {};
            this.displayAccounts(this.accountsData);
            this.populateAccountSelect();
        } catch (error) {
            this.showMessage(`加载账户数据失败: ${error.message}`, 'error');
        }
    }

    // 显示账户表格
    displayAccounts(accounts) {
        const tbody = document.getElementById('accounts-tbody');
        tbody.innerHTML = '';

        if (!accounts || Object.keys(accounts).length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" style="text-align: center;">暂无账户数据</td></tr>';
            return;
        }

        Object.entries(accounts).forEach(([accountId, accountData]) => {
            const cash = accountData.cash ? Object.values(accountData.cash).reduce((sum, val) => sum + val, 0) : 0;
            const holdingsCount = accountData.holdings ? Object.keys(accountData.holdings).length : 0;
            const meta = accountData.meta || {};
            
            const row = document.createElement('tr');
            row.innerHTML = `
                <td><strong>${accountId}</strong></td>
                <td>${cash.toFixed(2)}</td>
                <td>${holdingsCount}</td>
                <td>${meta.Country || 'N/A'}</td>
                <td>${meta.currency || 'N/A'}</td>
                <td>${meta.lastUpdated ? new Date(meta.lastUpdated).toLocaleString() : 'N/A'}</td>
                <td>
                    <button class="btn btn-primary" onclick="app.editAccount('${accountId}')">编辑</button>
                    <button class="btn btn-danger" onclick="app.deleteAccount('${accountId}')">删除</button>
                </td>
            `;
            tbody.appendChild(row);
        });
    }

    // 填充账户选择下拉框
    populateAccountSelect() {
        const select = document.getElementById('account-select');
        select.innerHTML = '<option value="">选择账户...</option>';
        
        Object.keys(this.accountsData).forEach(accountId => {
            const option = document.createElement('option');
            option.value = accountId;
            option.textContent = accountId;
            select.appendChild(option);
        });
    }

    /*
    // 加载持仓数据
    async loadHoldings(accountId) {
        try {
            const accountData = this.accountsData[accountId];
            if (!accountData || !accountData.holdings) {
                document.getElementById('holdings-content').innerHTML = '<p>该账户暂无持仓数据</p>';
                return;
            }

            const holdings = accountData.holdings;
            let html = `
                <h3>账户 ${accountId} 的持仓</h3>
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>代码</th>
                            <th>公司</th>
                            <th>持仓数量</th>
                            <th>成本价</th>
                            <th>货币</th>
                            <th>交易所</th>
                        </tr>
                    </thead>
                    <tbody>
            `;

            Object.entries(holdings).forEach(([tickerKey, holding]) => {
                html += `
                    <tr>
                        <td>${holding.ticker || tickerKey}</td>
                        <td>${holding.company || 'N/A'}</td>
                        <td>${holding.holding || 0}</td>
                        <td>${holding.costPerShare ? holding.costPerShare.toFixed(2) : 'N/A'}</td>
                        <td>${holding.currency || 'N/A'}</td>
                        <td>${holding.exchangeCode || 'N/A'}</td>
                    </tr>
                `;
            });

            html += '</tbody></table>';
            document.getElementById('holdings-content').innerHTML = html;
        } catch (error) {
            this.showMessage(`加载持仓数据失败: ${error.message}`, 'error');
        }
    }
    */
   // 修改 loadHoldings 方法以支持编辑功能
async loadHoldings(accountId) {
    try {
        const accountData = this.accountsData[accountId];
        if (!accountData || !accountData.holdings) {
            document.getElementById('holdings-content').innerHTML = '<p>该账户暂无持仓数据</p>';
            return;
        }

        const holdings = accountData.holdings;
        let html = `
            <h3>账户 ${accountId} 的持仓</h3>
            <table class="data-table holdings-table">
                <thead>
                    <tr>
                        <th>代码</th>
                        <th>公司</th>
                        <th>持仓数量</th>
                        <th>成本价</th>
                        <th>货币</th>
                        <th>交易所</th>
                        <th>操作</th>
                    </tr>
                </thead>
                <tbody>
        `;

        Object.entries(holdings).forEach(([tickerKey, holding]) => {
            html += `
                <tr data-holding-key="${tickerKey}" data-account-id="${accountId}">
                    <td class="ticker-cell">${holding.ticker || tickerKey}</td>
                    <td class="company-cell">${holding.company || 'N/A'}</td>
                    <td class="quantity-cell editable-cell">
                        <span class="display-value">${holding.holding || 0}</span>
                        <span class="edit-icon">✏️</span>
                        <input type="number" class="editable-input hidden" value="${holding.holding || 0}" min="0" step="1">
                    </td>
                    <td class="cost-cell editable-cell">
                        <span class="display-value">${holding.costPerShare ? holding.costPerShare.toFixed(3) : '0.000'}</span>
                        <span class="edit-icon">✏️</span>
                        <input type="number" class="editable-input hidden" value="${holding.costPerShare || 0}" min="0" step="0.001">
                    </td>
                    <td>${holding.currency || 'N/A'}</td>
                    <td>${holding.exchangeCode || 'N/A'}</td>
                    <td>
                        <button class="btn btn-danger delete-holding" onclick="app.deleteHolding('${accountId}', '${tickerKey}')">删除</button>
                    </td>
                </tr>
            `;
        });

        html += '</tbody></table>';
        document.getElementById('holdings-content').innerHTML = html;
        
        // 初始化编辑功能
        this.initHoldingEditListeners();
    } catch (error) {
        this.showMessage(`加载持仓数据失败: ${error.message}`, 'error');
    }
}


initHoldingEditListeners() {
    // 悬停显示编辑图标
    document.querySelectorAll('.editable-cell').forEach(cell => {
        cell.addEventListener('mouseenter', this.showEditIcon.bind(this));
        cell.addEventListener('mouseleave', this.hideEditIcon.bind(this));
    });
    
    // 点击编辑图标进入编辑模式
    document.querySelectorAll('.edit-icon').forEach(icon => {
        icon.addEventListener('click', this.startEditing.bind(this));
    });
    
    // 输入框事件监听
    document.querySelectorAll('.editable-input').forEach(input => {
        input.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                this.finishEditing(e.target);
            } else if (e.key === 'Escape') {
                this.cancelEditing(e.target);
            }
        });
        
        input.addEventListener('blur', (e) => {
            this.finishEditing(e.target);
        });
    });
}

showEditIcon(e) {
    const cell = e.currentTarget;
    const editIcon = cell.querySelector('.edit-icon');
    editIcon.style.visibility = 'visible';
}

hideEditIcon(e) {
    const cell = e.currentTarget;
    const editIcon = cell.querySelector('.edit-icon');
    if (!cell.querySelector('.editable-input:focus')) {
        editIcon.style.visibility = 'hidden';
    }
}

startEditing(e) {
    const cell = e.currentTarget.parentElement;
    const displayValue = cell.querySelector('.display-value');
    const input = cell.querySelector('.editable-input');
    
    displayValue.classList.add('hidden');
    input.classList.remove('hidden');
    input.focus();
    input.select();
    
    // 隐藏编辑图标
    cell.querySelector('.edit-icon').style.visibility = 'hidden';
}

async finishEditing(input) {
    const cell = input.parentElement;
    const displayValue = cell.querySelector('.display-value');
    const row = cell.closest('tr');
    const accountId = row.dataset.accountId;
    const holdingKey = row.dataset.holdingKey;
    const fieldType = cell.classList.contains('quantity-cell') ? 'quantity' : 'cost';
    
    // 数据验证
    const value = fieldType === 'quantity' ? parseInt(input.value) : parseFloat(input.value);
    
    if (isNaN(value)) {
        this.showMessage('请输入有效的数值', 'error');
        this.cancelEditing(input);
        return;
    }
    
    if (value < 0) {
        this.showMessage('数值不能为负数', 'error');
        this.cancelEditing(input);
        return;
    }
    
    if (fieldType === 'quantity' && !Number.isInteger(value)) {
        this.showMessage('持仓数量必须为整数', 'error');
        this.cancelEditing(input);
        return;
    }
    
    try {
        // 更新显示值
        displayValue.textContent = fieldType === 'quantity' ? value : value.toFixed(3);
        
        // 更新数据库
        const updateData = {};
        if (fieldType === 'quantity') {
            updateData[`accounts/${accountId}/holdings/${holdingKey}/holding`] = value;
        } else {
            updateData[`accounts/${accountId}/holdings/${holdingKey}/costPerShare`] = parseFloat(value.toFixed(3));
        }
        
        updateData[`accounts/${accountId}/holdings/${holdingKey}/lastUpdated`] = new Date().toISOString();
        updateData[`accounts/${accountId}/holdings/${holdingKey}/updatedBy`] = this.currentUser.email;
        
        await this.db.ref().update(updateData);
        
        // 退出编辑模式
        input.classList.add('hidden');
        displayValue.classList.remove('hidden');
        
        this.showMessage('✅ 持仓更新成功', 'success');
    } catch (error) {
        this.showMessage(`更新失败: ${error.message}`, 'error');
        this.cancelEditing(input);
    }
}

cancelEditing(input) {
    const cell = input.parentElement;
    const displayValue = cell.querySelector('.display-value');
    
    input.value = displayValue.textContent; // 恢复原值
    input.classList.add('hidden');
    displayValue.classList.remove('hidden');
}

async deleteHolding(accountId, holdingKey) {
    if (!confirm(`确定要删除持仓 ${holdingKey} 吗？此操作不可撤销。`)) {
        return;
    }
    
    try {
        await this.db.ref(`accounts/${accountId}/holdings/${holdingKey}`).remove();
        this.showMessage('✅ 持仓删除成功', 'success');
        this.loadHoldings(accountId);
    } catch (error) {
        this.showMessage(`删除失败: ${error.message}`, 'error');
    }
}

    // 显示添加账户模态框
    showAddAccountModal() {
        document.getElementById('modal-title').textContent = '添加新账户';
        document.getElementById('account-form').reset();
        document.getElementById('edit-account-id').value = '';
        this.showModal();
    }

    // 编辑账户
    editAccount(accountId) {
        const accountData = this.accountsData[accountId];
        if (!accountData) return;

        document.getElementById('modal-title').textContent = `编辑账户 ${accountId}`;
        document.getElementById('edit-account-id').value = accountId;
        
        const cash = accountData.cash ? accountData.cash.CNY || 0 : 0;
        const meta = accountData.meta || {};
        
        document.getElementById('account-cash').value = cash;
        document.getElementById('account-country').value = meta.Country || 'CN';
        document.getElementById('account-currency').value = meta.currency || 'CNY';
        
        this.showModal();
    }

    // 保存账户
    async saveAccount() {
        const accountId = document.getElementById('edit-account-id').value;
        const cash = parseFloat(document.getElementById('account-cash').value);
        const country = document.getElementById('account-country').value;
        const currency = document.getElementById('account-currency').value;

        if (!accountId) {
            this.showMessage('请输入账户ID', 'error');
            return;
        }

        try {
            const accountData = {
                cash: { CNY: cash },
                debt: { CNY: 0 },
                holdings: this.accountsData[accountId]?.holdings || {},
                meta: {
                    Country: country,
                    currency: currency,
                    lastUpdated: new Date().toISOString(),
                    updatedBy: this.currentUser.email
                }
            };

            await this.db.ref(`accounts/${accountId}`).set(accountData);
            this.hideModal();
            this.showMessage('✅ 账户保存成功', 'success');
            this.loadAccounts();
        } catch (error) {
            this.showMessage(`保存失败: ${error.message}`, 'error');
        }
    }

    // 删除账户
    async deleteAccount(accountId) {
        if (!confirm(`确定要删除账户 ${accountId} 吗？此操作不可撤销。`)) {
            return;
        }

        try {
            await this.db.ref(`accounts/${accountId}`).remove();
            this.showMessage('✅ 账户删除成功', 'success');
            this.loadAccounts();
        } catch (error) {
            this.showMessage(`删除失败: ${error.message}`, 'error');
        }
    }

    // 显示模态框
    showModal() {
        document.getElementById('modal').classList.remove('hidden');
    }

    // 隐藏模态框
    hideModal() {
        document.getElementById('modal').classList.add('hidden');
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
const app = new FirebaseAdminApp();
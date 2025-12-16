// toolSmith/toolRunner.js
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { fork } = require('child_process');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

const scriptDir = __dirname;
const currentScript = 'toolRunner.js';

// ASCII Art Header
function printHeader() {
    console.log('\n==================================================');
    console.log('🛠️  TOOL SMITH RUNNER 🛠️');
    console.log('==================================================');
}

/**
 * 获取可执行脚本列表
 */
function getAvailableScripts() {
    try {
        const files = fs.readdirSync(scriptDir);
        return files.filter(file =>
            file.endsWith('.js') &&
            file !== currentScript
        );
    } catch (error) {
        console.error('❌ 无法读取目录:', error.message);
        return [];
    }
}

/**
 * 询问问题
 */
function askQuestion(query) {
    return new Promise(resolve => rl.question(query, resolve));
}

/**
 * 执行脚本
 */
function runScript(scriptName, args) {
    return new Promise((resolve, reject) => {
        console.log(`\n🚀 正在启动 ${scriptName}...\n`);
        console.log('---------------- 脚本输出 START ----------------');

        const scriptPath = path.join(scriptDir, scriptName);

        // 使用 fork 创建子进程，以便隔离执行环境
        const child = fork(scriptPath, args, {
            stdio: 'inherit' // 直接将子进程的 IO 管道接到父进程
        });

        child.on('exit', (code) => {
            console.log('\n---------------- 脚本输出 END ----------------');
            if (code === 0) {
                console.log(`✅ 脚本执行成功 (Exit Code: 0)`);
                resolve(true);
            } else {
                console.error(`❌ 脚本执行失败 (Exit Code: ${code})`);
                resolve(false);
            }
        });

        child.on('error', (err) => {
            console.error(`❌ 无法启动脚本: ${err.message}`);
            resolve(false);
        });
    });
}

/**
 * 主循环
 */
async function main() {
    printHeader();

    while (true) {
        const scripts = getAvailableScripts();

        if (scripts.length === 0) {
            console.log('⚠️  未找到任何工具脚本。');
            break;
        }

        console.log('\n可用工具列表:');
        scripts.forEach((script, index) => {
            console.log(`  [${index + 1}] ${script}`);
        });
        console.log(`  [0] 退出`);

        const selection = await askQuestion('\n请选择要执行的工具编号: ');
        const index = parseInt(selection, 10);

        if (isNaN(index)) {
            console.log('❌ 无效输入，请输入数字。');
            continue;
        }

        if (index === 0) {
            console.log('👋 再见！');
            break;
        }

        if (index < 1 || index > scripts.length) {
            console.log('❌ 编号超出范围。');
            continue;
        }

        const selectedScript = scripts[index - 1];
        console.log(`\n您选择了: ${selectedScript}`);

        // 询问额外参数
        const argsInput = await askQuestion('请输入脚本参数 (空格分隔，直接回车跳过): ');
        const args = argsInput.trim() ? argsInput.trim().split(/\s+/) : [];

        // 确认执行
        const confirm = await askQuestion('确认执行? (y/n) [y]: ');
        if (confirm.toLowerCase() === 'n') {
            console.log('🚫 已取消');
            continue;
        }

        // 执行脚本
        await runScript(selectedScript, args);

        // 询问是否继续
        const cont = await askQuestion('\n是否继续执行其他工具? (y/n) [y]: ');
        if (cont.toLowerCase() === 'n') {
            console.log('👋 再见！');
            break;
        }
    }

    rl.close();
}

// 启动
if (require.main === module) {
    main().catch(error => {
        console.error('Fatal Error:', error);
        rl.close();
    });
}

module.exports = main;

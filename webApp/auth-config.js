// 授权的Gmail账户列表
const AUTHORIZED_USERS = [
    'alexszhang@gmail.com'
    
    // 添加更多授权邮箱
];

// 检查用户是否授权
function isUserAuthorized(email) {
    return AUTHORIZED_USERS.includes(email);
}
importScripts('https://www.gstatic.com/firebasejs/9.0.0/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/9.0.0/firebase-messaging-compat.js');

// 1. 初始化 Firebase (替换为你的配置)
const firebaseConfig = {
  apiKey: "AIzaSyAXlqDll3KTeuwuE7bIpo6fMepouHHhILs",
  authDomain: "outpostmessageproxy.firebaseapp.com",
  projectId: "outpostmessageproxy",
  storageBucket: "outpostmessageproxy.firebasestorage.app",
  messagingSenderId: "788806511022",
  appId: "1:788806511022:web:0ccd2bb10d8eee8638403f"
};

firebase.initializeApp(firebaseConfig);
const messaging = firebase.messaging();

// 2. 监听后台消息
messaging.onBackgroundMessage(function(payload) {
  console.log('[SW] 后台收到消息:', payload);

  // 1. 解析自定义数据
  // 注意：payload.data 里的都是字符串
  const targetUrl = payload.data.url || '/'; 
  
  // 2. 自定义通知外观
  const notificationTitle = payload.notification.title;
  const notificationOptions = {
    body: payload.notification.body,
    icon: '/icons/icon-192.png',
    badge: '/icons/badge.png', // 安卓状态栏的小图标
    data: { 
      // 关键！把需要跳转的 URL 藏在这里，点击事件要用
      url: targetUrl,
      timestamp: Date.now()
    },
    // 3. 交互按钮 (Action Buttons) - 仅限部分支持的浏览器
    actions: [
      {action: 'open', title: '查看详情'},
      {action: 'close', title: '忽略'}
    ]
  };

  return self.registration.showNotification(notificationTitle, notificationOptions);
});



// 追加在 firebase-messaging-sw.js 底部
self.addEventListener('notificationclick', function(event) {
  console.log('[SW] 通知被点击');
  
  // 1. 关闭通知弹窗 (必做，否则它会一直停在那)
  event.notification.close();

  // 2. 获取我们在阶段一里埋进去的数据
  const targetUrl = event.notification.data.url || '/';

  // 3. 智能导航逻辑 (这是 PWA 的精华)
  event.waitUntil(
    clients.matchAll({type: 'window', includeUncontrolled: true}).then(function(windowClients) {
      // A. 检查是否已经有打开的窗口
      for (let i = 0; i < windowClients.length; i++) {
        const client = windowClients[i];
        // 如果找到了，且 URL 匹配 (或者只是想聚焦同一个 App)
        if (client.url.includes(targetUrl) && 'focus' in client) {
          return client.focus(); // 仅仅聚焦，或者 client.navigate(targetUrl) 刷新
        }
      }
      // B. 如果没有打开的窗口，则打开新窗口
      if (clients.openWindow) {
        return clients.openWindow(targetUrl);
      }
    })
  );
});
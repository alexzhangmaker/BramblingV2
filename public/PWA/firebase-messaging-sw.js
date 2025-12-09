importScripts('https://www.gstatic.com/firebasejs/9.0.0/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/9.0.0/firebase-messaging-compat.js');

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

// --- IndexedDB 配置 (版本号升级为 2) ---
const DB_NAME = 'OutpostPWA_DB';
const DB_VERSION = 2; // ✅ 升级版本
const STORE_INBOX = 'inbox';
const STORE_CONFIG = 'config'; // ✅ 新增配置存储

function getDB() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    
    request.onupgradeneeded = (event) => {
      const db = event.target.result;
      // 创建收件箱 Store
      if (!db.objectStoreNames.contains(STORE_INBOX)) {
        db.createObjectStore(STORE_INBOX, { keyPath: 'id', autoIncrement: true });
      }
      // ✅ 创建配置 Store (键值对模式)
      if (!db.objectStoreNames.contains(STORE_CONFIG)) {
        db.createObjectStore(STORE_CONFIG, { keyPath: 'key' });
      }
    };

    request.onsuccess = (event) => resolve(event.target.result);
    request.onerror = (e) => reject(e);
  });
}

// 保存消息逻辑
async function saveMsgToDB(msgData) {
  try {
    const db = await getDB();
    const tx = db.transaction(STORE_INBOX, 'readwrite');
    const store = tx.objectStore(STORE_INBOX);
    
    store.add({
      title: msgData.notification.title,
      body: msgData.notification.body,
      url: msgData.data.url || '/',
      timestamp: Date.now(),
      read: false
    });
    
    return tx.complete;
  } catch (err) {
    console.error('[SW] DB Error:', err);
  }
}

// 监听后台消息
messaging.onBackgroundMessage(function(payload) {
  console.log('[SW] 后台收到消息:', payload);
  saveMsgToDB(payload);

  const notificationTitle = payload.notification.title;
  const notificationOptions = {
    body: payload.notification.body,
    icon: 'https://cdn-icons-png.flaticon.com/512/1041/1041888.png',
    data: { url: payload.data.url || '/' }
  };

  return self.registration.showNotification(notificationTitle, notificationOptions);
});

// 点击跳转
self.addEventListener('notificationclick', function(event) {
  event.notification.close();
  const targetUrl = event.notification.data.url;

  event.waitUntil(
    clients.matchAll({type: 'window', includeUncontrolled: true}).then(function(windowClients) {
      for (let i = 0; i < windowClients.length; i++) {
        const client = windowClients[i];
        if (client.url.includes(targetUrl) && 'focus' in client) {
          return client.focus();
        }
      }
      if (clients.openWindow) return clients.openWindow(targetUrl);
    })
  );
});
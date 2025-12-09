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

// --- IndexedDB 辅助函数 (用于保存消息) ---
const DB_NAME = 'OutpostPWA_DB';
const STORE_NAME = 'inbox';

function saveMsgToDB(msgData) {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, 1);
    
    request.onupgradeneeded = (event) => {
      const db = event.target.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME, { keyPath: 'id', autoIncrement: true });
      }
    };

    request.onsuccess = (event) => {
      const db = event.target.result;
      const tx = db.transaction(STORE_NAME, 'readwrite');
      const store = tx.objectStore(STORE_NAME);
      
      // 构造要存储的数据对象
      const record = {
        title: msgData.notification.title,
        body: msgData.notification.body,
        url: msgData.data.url || '/',
        timestamp: Date.now(),
        read: false
      };
      
      store.add(record);
      tx.oncomplete = () => {
        db.close();
        resolve(record);
      };
    };
    
    request.onerror = (e) => reject(e);
  });
}

// --- 监听后台消息 ---
messaging.onBackgroundMessage(function(payload) {
  console.log('[SW] 后台收到消息:', payload);

  // 1. 保存到 IndexedDB (这是 Inbox 的关键)
  saveMsgToDB(payload).then(() => {
    console.log('[SW] 消息已保存到 Inbox');
  }).catch(err => console.error('[SW] 保存失败', err));

  // 2. 正常弹出通知
  const notificationTitle = payload.notification.title;
  const notificationOptions = {
    body: payload.notification.body,
    icon: 'https://cdn-icons-png.flaticon.com/512/1041/1041888.png',
    data: { 
      url: payload.data.url || '/',
    }
  };

  return self.registration.showNotification(notificationTitle, notificationOptions);
});

// --- 点击通知跳转 ---
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
      if (clients.openWindow) {
        return clients.openWindow(targetUrl);
      }
    })
  );
});
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
  console.log('[SW] 收到后台消息: ', payload);

  const notificationTitle = payload.notification.title;
  const notificationOptions = {
    body: payload.notification.body,
    icon: 'https://cdn-icons-png.flaticon.com/512/1041/1041888.png'
  };

  self.registration.showNotification(notificationTitle, notificationOptions);
});
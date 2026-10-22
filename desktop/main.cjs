const { app, BrowserWindow, dialog, shell } = require('electron');
const { spawn } = require('node:child_process');
const path = require('node:path');
const http = require('node:http');
let server;
let mainWindow;
const PORT = 8787;
function waitForServer(attempt = 0) {
  http.get('http://127.0.0.1:' + PORT + '/', res => {
    res.resume();
    if (mainWindow && !mainWindow.isDestroyed()) mainWindow.loadURL('http://127.0.0.1:' + PORT + '/');
  }).on('error', () => {
    if (attempt >= 50) {
      dialog.showErrorBox('起動できませんでした', '中継サーバーに接続できません。8787番ポートが使用中でないか確認してください。');
      return;
    }
    setTimeout(() => waitForServer(attempt + 1), 200);
  });
}
app.whenReady().then(() => {
  const serverDir = path.resolve(__dirname, '../uta-timer-server');
  server = spawn(process.execPath, [path.join(serverDir, 'server.js')], {
    cwd: serverDir,
    env: { ...process.env, ELECTRON_RUN_AS_NODE: '1', WS_PORT: String(PORT) },
    stdio: 'pipe',
    windowsHide: true
  });
  server.on('error', err => dialog.showErrorBox('サーバー起動エラー', err.message));
  server.stderr.on('data', chunk => console.error(String(chunk)));
  mainWindow = new BrowserWindow({
    width: 780, height: 880, minWidth: 540, minHeight: 560,
    title: 'ふえニャン', webPreferences: { nodeIntegration: false, contextIsolation: true, sandbox: true }
  });
  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    if (/^https?:\/\//.test(url)) shell.openExternal(url);
    return { action: 'deny' };
  });
  waitForServer();
});
app.on('before-quit', () => { if (server && !server.killed) server.kill(); });
app.on('window-all-closed', () => app.quit());

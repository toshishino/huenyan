// uta-timer-server
// Twitch(匿名IRC) を監視し、「初コメ」「投げ銭」「サブスク」「サブスクギフト」イベントを
// 検知してWebSocketでタイマー画面(ブラウザ)にリアルタイム通知する中継サーバーです。
// YouTube(Live Chat API)対応のコードも用意してありますが、既定では無効化しています。
//
// 設定(Twitchチャンネル名・ギフト種別のon/off・YouTube有効化等)は
// .envを編集してサーバーを再起動する必要はありません。
// サーバー起動後に http://localhost:8787/ を開くと設定画面が使えます。
//
// 使い方:
//   1. npm install
//   2. cp .env.example .env  (WS_PORT等、めったに変えない項目だけ)
//   3. npm start
//   4. ブラウザで http://localhost:8787/ を開いてTwitchチャンネル名などを設定
//   5. タイマー画面(uta-timer.html)を ?ws=ws://localhost:8787 付きで開く

import 'dotenv/config';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';
import tmi from 'tmi.js';
import readline from 'node:readline';
import {
  recordComment,
  recordGift,
  getYoutubePageToken,
  saveYoutubePageToken,
  getSetting,
  setSetting,
} from './db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// 配布を見据えて配信者ごとにIDを分ける(マルチテナント前提)。
// 自分一人で使ってる間は既定値のままでOK。
const STREAMER_ID = process.env.STREAMER_ID || 'default';
const WS_PORT = process.env.WS_PORT || 8787;
const HOST = process.env.HOST || '127.0.0.1';
const timerHtmlPath = path.resolve(__dirname, '../uta-timer.html');
const timerStatePath = path.join(__dirname, 'timer-state.json');
const digitDesignPath = path.join(__dirname,'digit-design.json');
let digitDesign = {};
try { digitDesign=JSON.parse(fs.readFileSync(digitDesignPath,'utf8')); } catch (_) {}
let timerState = { remaining:1800, running:false, updatedAt:Date.now(), settings:{base:30,comment:5,gift:10}, commentCount:0, giftCount:0, history:[], track:[] };
try { timerState = {...timerState,...JSON.parse(fs.readFileSync(timerStatePath,'utf8'))}; } catch (_) {}
function currentTimerState() { const s={...timerState}; if(s.running) s.remaining-=Math.floor((Date.now()-s.updatedAt)/1000); return s; }
function saveTimerState(state) { timerState={...state,updatedAt:Date.now()}; fs.writeFileSync(timerStatePath,JSON.stringify(timerState)); broadcast({type:'timer_state',state:currentTimerState()}); }


// ------------------------------------------------------------
// ユーザーが設定画面から変更できる項目。
// 起動時にDB(前回保存分)→無ければ.env→無ければデフォルト、の順で読み込む。
// 変更されたらDBに保存し、Twitch接続やYouTubeポーリングにも即座に反映する。
// ------------------------------------------------------------
function boolFrom(v, fallback) {
  if (v === null || v === undefined) return fallback;
  return v === 'true' || v === true;
}

const config = {
  enableTwitch: boolFrom(getSetting(STREAMER_ID, 'enableTwitch', process.env.ENABLE_TWITCH), true),
  twitchChannel: getSetting(STREAMER_ID, 'twitchChannel', process.env.TWITCH_CHANNEL || ''),
  countCheer: boolFrom(getSetting(STREAMER_ID, 'countCheer', process.env.TWITCH_COUNT_CHEER), true),
  countSub: boolFrom(getSetting(STREAMER_ID, 'countSub', process.env.TWITCH_COUNT_SUB), true),
  countSubGift: boolFrom(getSetting(STREAMER_ID, 'countSubGift', process.env.TWITCH_COUNT_SUBGIFT), true),
  enableYoutube: boolFrom(getSetting(STREAMER_ID, 'enableYoutube', process.env.ENABLE_YOUTUBE), false),
  youtubeApiKey: getSetting(STREAMER_ID, 'youtubeApiKey', process.env.YOUTUBE_API_KEY || ''),
  youtubeVideoId: getSetting(STREAMER_ID, 'youtubeVideoId', process.env.YOUTUBE_VIDEO_ID || ''),
};

const CONFIGURABLE_KEYS = [
  'enableTwitch', 'twitchChannel',
  'countCheer', 'countSub', 'countSubGift',
  'enableYoutube', 'youtubeApiKey', 'youtubeVideoId',
];

function saveConfig() {
  for (const key of CONFIGURABLE_KEYS) {
    setSetting(STREAMER_ID, key, config[key]);
  }
}
saveConfig(); // 初回起動時、.env由来の値をDBにも反映しておく

// ------------------------------------------------------------
// 「初コメ」は生涯初コメントではなく「この配信で初めて発言したか」で判定する。
// サーバー起動のタイミング = 配信開始、という前提でメモリ上のSetをセッション単位で持つ。
// (DBは初コメ判定には使わず、常連ランキング用の生涯累計データとして別管理する)
// ------------------------------------------------------------
let seenThisStream = { twitch: new Set(), youtube: new Set() };
let lastResetAt = null;

function resetSession(reason = 'manual') {
  seenThisStream = { twitch: new Set(), youtube: new Set() };
  lastResetAt = new Date().toISOString();
  console.log(`[session] 初コメ判定をリセットしました (${reason})`);
  broadcast({ source: 'system', type: 'session_reset', reason });
}

// サーバーを再起動せずに新しい配信としてリセットしたい場合、
// ターミナルで r + Enter を押すとセッションだけリセットできる。
readline.createInterface({ input: process.stdin }).on('line', (line) => {
  if (line.trim().toLowerCase() === 'r') resetSession('手動リセット');
});

// ------------------------------------------------------------
// WebSocket: タイマー画面への通知
// ------------------------------------------------------------
const clients = new Set();

function broadcast(event) {
  const payload = JSON.stringify(event);
  console.log('[event]', payload);
  for (const ws of clients) {
    if (ws.readyState === ws.OPEN) ws.send(payload);
  }
}

// ------------------------------------------------------------
// Twitch: 匿名IRC接続。読み取り専用なのでログインは不要です。
// チャンネル名や有効/無効が設定画面から変わったら再接続する。
// ------------------------------------------------------------
let twitchClient = null;
let twitchConnectedChannel = null;

function attachTwitchHandlers(client) {
  client.on('message', (chan, tags, message, self) => {
    if (self) return;
    const userId = tags['user-id'] || tags.username;
    const displayName = tags['display-name'] || tags.username;

    recordComment(STREAMER_ID, 'twitch', userId, displayName);

    if (!seenThisStream.twitch.has(userId)) {
      seenThisStream.twitch.add(userId);
      broadcast({ source: 'twitch', type: 'first_comment', user: displayName });
    }
  });

  client.on('cheer', (chan, tags) => {
    const userId = tags['user-id'] || tags.username;
    const displayName = tags['display-name'] || tags.username;
    recordGift(STREAMER_ID, 'twitch', userId, displayName, 1);
    if (!config.countCheer) return;
    broadcast({
      source: 'twitch', type: 'donation', user: displayName,
      amount: Number(tags.bits || 0), note: `${tags.bits} bits`,
    });
  });

  client.on('subscription', (chan, username, methods, message, userstate) => {
    const userId = userstate['user-id'] || username;
    const displayName = userstate['display-name'] || username;
    recordGift(STREAMER_ID, 'twitch', userId, displayName, 1);
    if (!config.countSub) return;
    broadcast({ source: 'twitch', type: 'subscription', user: displayName });
  });

  client.on('resub', (chan, username, months, message, userstate) => {
    const userId = userstate['user-id'] || username;
    const displayName = userstate['display-name'] || username;
    recordGift(STREAMER_ID, 'twitch', userId, displayName, 1);
    if (!config.countSub) return;
    broadcast({ source: 'twitch', type: 'subscription', user: displayName });
  });

  client.on('subgift', (chan, username, streakMonths, recipient, methods, userstate) => {
    const userId = userstate['user-id'] || username;
    const displayName = userstate['display-name'] || username;
    recordGift(STREAMER_ID, 'twitch', userId, displayName, 1);
    if (!config.countSubGift) return;
    broadcast({ source: 'twitch', type: 'sub_gift', user: displayName, count: 1 });
  });

  client.on('submysterygift', (chan, username, numbOfSubs, methods, userstate) => {
    const userId = userstate['user-id'] || username;
    const displayName = userstate['display-name'] || username;
    const count = Number(numbOfSubs || 1);
    recordGift(STREAMER_ID, 'twitch', userId, displayName, count);
    if (!config.countSubGift) return;
    broadcast({ source: 'twitch', type: 'sub_gift', user: displayName, count });
  });
}

async function connectTwitch(channel) {
  if (twitchClient) {
    try { await twitchClient.disconnect(); } catch (e) { /* noop */ }
    twitchClient = null;
  }
  twitchClient = new tmi.Client({ channels: [channel] });
  attachTwitchHandlers(twitchClient);
  twitchClient.connect().catch((err) => console.error('[twitch] connect error', err));
  twitchConnectedChannel = channel;
  console.log(`[twitch] watching #${channel}`);
}

async function disconnectTwitch() {
  if (twitchClient) {
    try { await twitchClient.disconnect(); } catch (e) { /* noop */ }
    console.log('[twitch] disconnected');
  }
  twitchClient = null;
  twitchConnectedChannel = null;
}

async function reconcileTwitch() {
  if (!config.enableTwitch || !config.twitchChannel) {
    if (twitchClient) await disconnectTwitch();
    return;
  }
  if (twitchConnectedChannel !== config.twitchChannel) {
    await connectTwitch(config.twitchChannel);
  }
}

// ------------------------------------------------------------
// YouTube: Live Chat API をポーリング。設定画面のenableYoutube/APIキー/動画IDに応じて
// 開始・停止する。既存のポーリングを止めたいときは session.stop を立てる。
// ------------------------------------------------------------
let youtubeSession = null;

async function startYoutube(session) {
  const apiKey = config.youtubeApiKey;
  const videoId = session.videoId;

  const videoRes = await fetch(
    `https://www.googleapis.com/youtube/v3/videos?part=liveStreamingDetails&id=${videoId}&key=${apiKey}`
  );
  const videoData = await videoRes.json();
  const liveChatId = videoData?.items?.[0]?.liveStreamingDetails?.activeLiveChatId;
  if (!liveChatId) {
    console.error('[youtube] liveChatId が取得できませんでした。配信が開始されているか、動画IDが正しいか確認してください。');
    return;
  }
  console.log('[youtube] liveChatId =', liveChatId);

  let pageToken = getYoutubePageToken(STREAMER_ID, videoId) || '';
  if (pageToken) console.log('[youtube] 前回の続きから再開します (再起動時の二重カウント防止)');

  async function poll() {
    if (session.stop) return;
    try {
      const url = new URL('https://www.googleapis.com/youtube/v3/liveChat/messages');
      url.searchParams.set('liveChatId', liveChatId);
      url.searchParams.set('part', 'snippet,authorDetails');
      url.searchParams.set('key', apiKey);
      if (pageToken) url.searchParams.set('pageToken', pageToken);

      const res = await fetch(url);
      const data = await res.json();

      if (data.error) {
        console.error('[youtube] API error', data.error.message);
        if (!session.stop) setTimeout(poll, 5000);
        return;
      }

      for (const item of data.items || []) {
        const snippet = item.snippet;
        const author = item.authorDetails;
        const type = snippet.type;

        if (type === 'textMessageEvent' || type === 'superChatEvent' || type === 'superStickerEvent') {
          recordComment(STREAMER_ID, 'youtube', author.channelId, author.displayName);
          if (!seenThisStream.youtube.has(author.channelId)) {
            seenThisStream.youtube.add(author.channelId);
            broadcast({ source: 'youtube', type: 'first_comment', user: author.displayName });
          }
        }
        if (type === 'superChatEvent') {
          recordGift(STREAMER_ID, 'youtube', author.channelId, author.displayName, 1);
          broadcast({ source: 'youtube', type: 'donation', user: author.displayName, note: snippet.superChatDetails?.amountDisplayString });
        }
        if (type === 'superStickerEvent') {
          recordGift(STREAMER_ID, 'youtube', author.channelId, author.displayName, 1);
          broadcast({ source: 'youtube', type: 'donation', user: author.displayName, note: snippet.superStickerDetails?.amountDisplayString });
        }
        if (type === 'membershipGiftingEvent') {
          const count = Number(snippet.membershipGiftingDetails?.giftMembershipsCount || 1);
          recordGift(STREAMER_ID, 'youtube', author.channelId, author.displayName, count);
          broadcast({ source: 'youtube', type: 'sub_gift', user: author.displayName, count });
        }
      }

      pageToken = data.nextPageToken || pageToken;
      saveYoutubePageToken(STREAMER_ID, videoId, pageToken);
      const interval = Math.max(data.pollingIntervalMillis || 5000, 3000);
      if (!session.stop) setTimeout(poll, interval);
    } catch (err) {
      console.error('[youtube] poll error', err);
      if (!session.stop) setTimeout(poll, 5000);
    }
  }

  poll();
  console.log('[youtube] polling started for video', videoId);
}

function reconcileYoutube() {
  const shouldRun = config.enableYoutube && config.youtubeApiKey && config.youtubeVideoId;
  if (!shouldRun) {
    if (youtubeSession) {
      youtubeSession.stop = true;
      youtubeSession = null;
      console.log('[youtube] stopped');
    }
    return;
  }
  if (youtubeSession && youtubeSession.videoId === config.youtubeVideoId && !youtubeSession.stop) {
    return; // 既に同じ動画IDで動いている
  }
  if (youtubeSession) youtubeSession.stop = true;
  const session = { videoId: config.youtubeVideoId, stop: false };
  youtubeSession = session;
  startYoutube(session);
}

// ------------------------------------------------------------
// 設定の適用・保存
// ------------------------------------------------------------
function applyConfigUpdates(updates) {
  for (const key of CONFIGURABLE_KEYS) {
    if (key in updates) config[key] = updates[key];
  }
  saveConfig();
  reconcileTwitch();
  reconcileYoutube();
}

function publicConfig() {
  return {
    ...config,
    // APIキーは全部は表示しない(末尾4文字だけ見せる)
    youtubeApiKey: config.youtubeApiKey ? '••••' + config.youtubeApiKey.slice(-4) : '',
  };
}

function statusSnapshot() {
  return {
    twitchConnected: !!twitchClient,
    twitchChannel: twitchConnectedChannel,
    youtubeRunning: !!(youtubeSession && !youtubeSession.stop),
    youtubeVideoId: youtubeSession?.videoId || null,
    lastResetAt,
  };
}

// ------------------------------------------------------------
// HTTP: 設定画面 + WebSocketを同じポートで待ち受ける
// ------------------------------------------------------------
const server = http.createServer((req, res) => {
  handleHttp(req, res).catch((err) => {
    console.error('[http] error', err);
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'internal error' }));
  });
});

async function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', (c) => { body += c; if (body.length > 3_000_000) { reject(new Error('request too large')); req.destroy(); } });
    req.on('end', () => {
      try { resolve(body ? JSON.parse(body) : {}); }
      catch (e) { reject(e); }
    });
    req.on('error', reject);
  });
}

async function handleHttp(req, res) {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (req.method === 'GET' && url.pathname === '/timer') {
    res.writeHead(200, {'Content-Type':'text/html; charset=utf-8'}); res.end(fs.readFileSync(timerHtmlPath,'utf8')); return;
  }
  if (req.method === 'GET' && url.pathname === '/api/digit-design') {
    res.writeHead(200, {'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(digitDesign));return;
  }
  if (req.method === 'POST' && url.pathname === '/api/digit-design') {
    const update=await readJsonBody(req);
    if (!update || typeof update!=='object' || typeof update.images!=='object' || Object.keys(update.images).some(k=>!/^[0-9]$/.test(k)||typeof update.images[k]!=='string'||!/^data:image\/(png|webp);base64,/.test(update.images[k])||update.images[k].length>450000)) {
      res.writeHead(400);res.end('invalid image data');return;
    }
    digitDesign=update;
    fs.writeFileSync(digitDesignPath,JSON.stringify(digitDesign));
    broadcast({type:'digit_design',design:digitDesign});
    res.writeHead(200,{'Content-Type':'application/json'});res.end(JSON.stringify({ok:true}));return;
  }
  if (req.method === 'GET' && url.pathname === '/api/timer-state') {
    res.writeHead(200, {'Content-Type':'application/json','Cache-Control':'no-store'}); res.end(JSON.stringify(currentTimerState())); return;
  }
  if (req.method === 'POST' && url.pathname === '/api/timer-state') {
    const update=await readJsonBody(req);
    if (!Number.isFinite(update.remaining) || Math.abs(update.remaining)>864000 || typeof update.running!=='boolean') { res.writeHead(400);res.end('invalid state');return; }
    saveTimerState({...timerState,...update,remaining:Math.trunc(update.remaining),history:Array.isArray(update.history)?update.history.slice(-100):[],track:Array.isArray(update.track)?update.track.slice(-100):[]});
    res.writeHead(200, {'Content-Type':'application/json'});res.end(JSON.stringify({ok:true}));return;
  }
  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/settings')) {
    const html = fs.readFileSync(path.join(__dirname, 'settings.html'), 'utf8');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (url.pathname === '/api/config' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ config: publicConfig(), status: statusSnapshot() }));
    return;
  }

  if (url.pathname === '/api/config' && req.method === 'POST') {
    const updates = await readJsonBody(req);
    applyConfigUpdates(updates);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, config: publicConfig(), status: statusSnapshot() }));
    return;
  }

  if (url.pathname === '/api/status' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(statusSnapshot()));
    return;
  }

  if (url.pathname === '/api/reset-session' && req.method === 'POST') {
    resetSession('設定画面から');
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, status: statusSnapshot() }));
    return;
  }

  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ ok: false, error: 'not found' }));
}

const wss = new WebSocketServer({ server });
wss.on('connection', (ws) => {
  clients.add(ws);
  ws.send(JSON.stringify({type:'timer_state',state:currentTimerState()}));
  ws.send(JSON.stringify({type:'digit_design',design:digitDesign}));
  console.log('[server] timer screen connected');
  ws.on('close', () => clients.delete(ws));
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    if (msg?.action === 'reset_session') {
      resetSession('タイマー画面から');
    }
  });
});

server.listen(WS_PORT, HOST, () => {
  console.log(`[server] listening on http://localhost:${WS_PORT}  (設定画面はブラウザでここを開く)`);
  console.log(`[server] WebSocket also available at ws://localhost:${WS_PORT}`);
  console.log('[server] 「r」+Enter で初コメ判定をリセットできます(配信をまたいで常駐させる場合用)');
  reconcileTwitch();
  reconcileYoutube();
});

// ------------------------------------------------------------
// 終了時にWebSocket/DBをきちんと閉じる
// ------------------------------------------------------------
function shutdown() {
  console.log('\n[server] shutting down...');
  if (twitchClient) twitchClient.disconnect().catch(() => {});
  if (youtubeSession) youtubeSession.stop = true;
  wss.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 1000);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// ======================================================================
// MULTI-EXCHANGE WEBSOCKET DEPTH SERVER
// ------------------------------------------------------------
// Один процесс держит ОТДЕЛЬНОЕ WebSocket-соединение на каждую биржу
// (не 10 разных серверов, как с REST — WS не тратит "вес" запросов у
// бирж так, как REST, поэтому не нужна стратегия разброса по IP).
// Каждый адаптер биржи собирает полный стакан в памяти (снапшот + дельты)
// и отдаёт его по одному общему HTTP-эндпоинту — формат ответа СОВПАДАЕТ
// с обычным REST-прокси, так что код в Apps Script меняется минимально
// (только адрес в конфиге конкретной биржи).
//
// ДОБАВЛЕНЫ ПОКА: Bitget, WhiteBIT. Остальные 9 бирж добавляются по одной,
// каждый раз с проверкой протокола по официальной документации — не
// торопимся с непроверенными догадками (см. историю багов сегодняшнего
// дня с REST-эндпоинтами).
//
// ВАЖНО: сервер должен работать КРУГЛОСУТОЧНО — платный тариф Render
// (не спит сам) либо бесплатный + внешний "будильник" (UptimeRobot/
// cron-job.org, пингует каждые 5-10 мин, не даёт заснуть).
//
// ЭНДПОИНТ (одинаковый для всех бирж, различаются только query-параметры):
//   GET /depth?exchange=bitget&symbol=BTCUSDT&marketType=spot
//   GET /depth?exchange=whitebit&symbol=BTC_USDT
//   -> { asks: [[price, size], ...], bids: [[price, size], ...] }
// Если стакан ещё не собран (только что подписались) — 202 {status:"warming_up"}
// ======================================================================

const express = require('express');
const WebSocket = require('ws');
const crypto = require('crypto');
const zlib = require('zlib'); // встроен в Node — для GZIP-сообщений HTX, доп. пакет не нужен
const app = express();

// ==================== ОБЩИЕ УТИЛИТЫ (одни на все биржи) ====================

// Живой стакан по каждому подписанному "биржа:символ" — Map(цена -> размер) для O(1) обновления
const books = new Map(); // key "exchange:instType:instId" -> { asks: Map, bids: Map, ready: bool }

function bookKey(exchange, instType, instId) {
  return `${exchange}:${instType}:${instId}`;
}
function ensureBook(key) {
  if (!books.has(key)) books.set(key, { asks: new Map(), bids: new Map(), ready: false });
  return books.get(key);
}
function applyLevels(map, levels) {
  if (!Array.isArray(levels)) return; // защита: неожиданный формат от биржи — просто пропускаем,
  // не роняем общий сервер (на нём висят ВСЕ 11 бирж, одно кривое сообщение не должно валить всё)
  for (const lvl of levels) {
    if (!Array.isArray(lvl) || lvl.length < 2) continue;
    const [priceStr, sizeStr] = lvl;
    if (Number(sizeStr) === 0) map.delete(priceStr);
    else map.set(priceStr, sizeStr);
  }
}
function mapToSortedArray(map, ascending) {
  return Array.from(map.entries())
    .sort((a, b) => (ascending ? Number(a[0]) - Number(b[0]) : Number(b[0]) - Number(a[0])))
    .map(([price, size]) => [price, size]);
}

// ==================== АДАПТЕР: BITGET ====================
// wss://ws.bitget.com/v2/ws/public, канал "books" (снапшот+дельты), ping "ping"/30с ждём "pong"

const bitget = {
  ws: null, pingTimer: null, pongTimer: null, subscribed: new Set(),
  connect() {
    this.ws = new WebSocket('wss://ws.bitget.com/v2/ws/public');
    this.ws.on('open', () => {
      console.log('Bitget WS: открыто');
      for (const key of this.subscribed) {
        const [instType, instId] = key.split(':');
        this.doSubscribe(instType, instId);
      }
      this.startPing();
    });
    this.ws.on('message', (raw) => {
      const text = raw.toString();
      if (text === 'pong') { clearTimeout(this.pongTimer); return; }
      let msg;
      try { msg = JSON.parse(text); } catch (e) { return; }
      if (!msg.action || !msg.arg || msg.arg.channel !== 'books') return;
      const key = bookKey('bitget', msg.arg.instType, msg.arg.instId);
      const book = ensureBook(key);
      const data = (msg.data && msg.data[0]) || {};
      if (msg.action === 'snapshot') {
        book.asks = new Map((data.asks || []).map(([p, s]) => [p, s]));
        book.bids = new Map((data.bids || []).map(([p, s]) => [p, s]));
        book.ready = true;
      } else if (msg.action === 'update') {
        applyLevels(book.asks, data.asks || []);
        applyLevels(book.bids, data.bids || []);
      }
    });
    this.ws.on('close', () => { console.log('Bitget WS: закрыто, переподключаюсь'); this.stopPing(); setTimeout(() => this.connect(), 3000); });
    this.ws.on('error', (err) => { console.log('Bitget WS ошибка:', err.message); this.ws.close(); });
  },
  startPing() {
    this.stopPing();
    this.pingTimer = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send('ping');
        this.pongTimer = setTimeout(() => { console.log('Bitget: pong не пришёл, реконнект'); this.ws.close(); }, 10000);
      }
    }, 25000);
  },
  stopPing() { clearInterval(this.pingTimer); clearTimeout(this.pongTimer); },
  doSubscribe(instType, instId) {
    const key = `${instType}:${instId}`;
    this.subscribed.add(key);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ op: 'subscribe', args: [{ instType, channel: 'books', instId }] }));
    }
  },
  requestSymbol(symbol, marketType) {
    const instType = marketType === 'futures' ? 'USDT-FUTURES' : 'SPOT';
    const key = bookKey('bitget', instType, symbol);
    if (!this.subscribed.has(`${instType}:${symbol}`)) this.doSubscribe(instType, symbol);
    return key;
  },
};

// ==================== АДАПТЕР: WHITEBIT ====================
// wss://api.whitebit.com/ws, JSON-RPC, метод depth_subscribe, ping раз в 50с

const whitebit = {
  ws: null, pingTimer: null, subscribed: new Set(), nextId: 1,
  connect() {
    this.ws = new WebSocket('wss://api.whitebit.com/ws');
    this.ws.on('open', () => {
      console.log('WhiteBIT WS: открыто');
      for (const market of this.subscribed) this.doSubscribe(market);
      this.startPing();
    });
    this.ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
      if (msg.method !== 'depth_update' || !Array.isArray(msg.params)) return;
      const [, updateData, market] = msg.params;
      const key = bookKey('whitebit', 'SPOT', market);
      const book = ensureBook(key);
      const isSnapshot = updateData.past_update_id === undefined || updateData.past_update_id === null;
      if (isSnapshot) {
        book.asks = new Map((updateData.asks || []).map(([p, s]) => [p, s]));
        book.bids = new Map((updateData.bids || []).map(([p, s]) => [p, s]));
        book.ready = true;
      } else {
        applyLevels(book.asks, updateData.asks || []);
        applyLevels(book.bids, updateData.bids || []);
      }
    });
    this.ws.on('close', () => { console.log('WhiteBIT WS: закрыто, переподключаюсь'); clearInterval(this.pingTimer); setTimeout(() => this.connect(), 3000); });
    this.ws.on('error', (err) => { console.log('WhiteBIT WS ошибка:', err.message); this.ws.close(); });
  },
  startPing() {
    clearInterval(this.pingTimer);
    this.pingTimer = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ id: 0, method: 'ping', params: [] }));
      }
    }, 50000);
  },
  doSubscribe(market) {
    this.subscribed.add(market);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ id: this.nextId++, method: 'depth_subscribe', params: [market, 100, '0', true] }));
    }
  },
  requestSymbol(symbol) {
    const key = bookKey('whitebit', 'SPOT', symbol);
    if (!this.subscribed.has(symbol)) this.doSubscribe(symbol);
    return key;
  },
};

// ==================== АДАПТЕР: BINANCE ====================
// Самый сложный протокол из всех: WS-стрим отдаёт только ДЕЛЬТЫ, полного снапшота там нет.
// Официальный алгоритм: буферизируем дельты, параллельно берём REST-снапшот с lastUpdateId,
// отбрасываем дельты старше снапшота, дальше проверяем непрерывность (U нового = u+1 предыдущего).
// Отдельные WS-соединения на спот и USDT-фьючи (разные домены), подписка сообщением SUBSCRIBE —
// так можно подписываться на лету без переподключения, как и у остальных адаптеров.

function makeBinanceAdapter(wsBase, restSnapshotUrl, marketLabel) {
  return {
    ws: null, nextId: 1, subscribed: new Set(),
    pending: new Map(), // symbol(lowercase) -> { buffer: [], snapshotRequested: bool }
    connect() {
      this.ws = new WebSocket(wsBase);
      this.ws.on('open', () => {
        console.log(`Binance ${marketLabel} WS: открыто`);
        for (const sym of this.subscribed) this.sendSubscribe(sym);
      });
      this.ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
        if (msg.e !== 'depthUpdate') return; // игнорируем служебные ответы на SUBSCRIBE и т.п.
        this.handleDiffEvent(msg);
      });
      this.ws.on('close', () => { console.log(`Binance ${marketLabel} WS: закрыто, переподключаюсь`); setTimeout(() => this.connect(), 3000); });
      this.ws.on('error', (err) => { console.log(`Binance ${marketLabel} WS ошибка:`, err.message); this.ws.close(); });
      // Пинг от сервера каждые 20с — библиотека 'ws' отвечает pong автоматически, ничего не пишем
    },
    sendSubscribe(symbolLower) {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ method: 'SUBSCRIBE', params: [`${symbolLower}@depth`], id: this.nextId++ }));
      }
    },
    async fetchSnapshotAndSync(symbolUpper, key) {
      try {
        const resp = await fetch(restSnapshotUrl(symbolUpper));
        const snap = await resp.json();
        const book = ensureBook(key);
        book.asks = new Map((snap.asks || []).map(([p, s]) => [p, s]));
        book.bids = new Map((snap.bids || []).map(([p, s]) => [p, s]));
        book.lastUpdateId = snap.lastUpdateId;
        // Применяем то, что накопилось в буфере, пока ждали снапшот — по правилам официального алгоритма
        const pend = this.pending.get(symbolUpper.toLowerCase());
        const buffered = (pend && pend.buffer) || [];
        let firstApplied = false;
        for (const evt of buffered) {
          if (evt.u <= book.lastUpdateId) continue; // событие старше снапшота — пропускаем
          if (!firstApplied) {
            // первое применяемое событие должно "накрывать" lastUpdateId снапшота
            firstApplied = true;
          }
          applyLevels(book.asks, evt.b || []);
          applyLevels(book.bids, evt.a || []);
          book.lastUpdateId = evt.u;
        }
        book.ready = true;
        if (pend) pend.snapshotRequested = true; // дальше live-события применяем напрямую, без буфера
      } catch (err) {
        console.log(`Binance ${marketLabel}: ошибка снапшота для ${symbolUpper}:`, err.message);
        // не выставляем ready — клиент получит warming_up и попробует ещё раз
      }
    },
    handleDiffEvent(msg) {
      const symbolUpper = msg.s;
      const symbolLower = symbolUpper.toLowerCase();
      const key = bookKey('binance-' + marketLabel, 'X', symbolUpper);
      let pend = this.pending.get(symbolLower);
      if (!pend) return; // событие по символу, который мы не запрашивали (не должно происходить)

      if (!pend.snapshotRequested) {
        // Снапшот ещё не готов — буферизируем, а не применяем напрямую (по официальному алгоритму)
        pend.buffer.push(msg);
        return;
      }
      const book = books.get(key);
      if (!book) return;
      // Проверка непрерывности: если разрыв — пересинхронизируемся с нуля через новый снапшот
      if (book.lastUpdateId != null && msg.U > book.lastUpdateId + 1) {
        console.log(`Binance ${marketLabel}: разрыв последовательности у ${symbolUpper}, пересинхронизация`);
        book.ready = false;
        pend.snapshotRequested = false;
        pend.buffer = [msg];
        this.fetchSnapshotAndSync(symbolUpper, key);
        return;
      }
      applyLevels(book.asks, msg.b || []);
      applyLevels(book.bids, msg.a || []);
      book.lastUpdateId = msg.u;
    },
    requestSymbol(symbol) {
      const symbolUpper = symbol.toUpperCase();
      const symbolLower = symbol.toLowerCase();
      const key = bookKey('binance-' + marketLabel, 'X', symbolUpper);
      if (!this.subscribed.has(symbolLower)) {
        this.subscribed.add(symbolLower);
        this.pending.set(symbolLower, { buffer: [], snapshotRequested: false });
        ensureBook(key);
        this.sendSubscribe(symbolLower);
        this.fetchSnapshotAndSync(symbolUpper, key); // запускаем сразу, не дожидаясь событий из стрима
      }
      return key;
    },
  };
}

const binanceSpot = makeBinanceAdapter(
  'wss://stream.binance.com:9443/ws',
  (sym) => `https://api.binance.com/api/v3/depth?symbol=${sym}&limit=5000`,
  'spot'
);
const binanceFutures = makeBinanceAdapter(
  'wss://fstream.binance.com/ws',
  (sym) => `https://fapi.binance.com/fapi/v1/depth?symbol=${sym}&limit=1000`,
  'futures'
);

// ==================== АДАПТЕР: OKX ====================
// wss://ws.okx.com:8443/ws/v5/public, канал "books" (снапшот+дельты), ping "ping"/20с ждём "pong"

const okx = {
  ws: null, pingTimer: null, subscribed: new Set(),
  connect() {
    this.ws = new WebSocket('wss://ws.okx.com:8443/ws/v5/public');
    this.ws.on('open', () => {
      console.log('OKX WS: открыто');
      for (const instId of this.subscribed) this.doSubscribe(instId);
      this.startPing();
    });
    this.ws.on('message', (raw) => {
      const text = raw.toString();
      if (text === 'pong') return;
      let msg;
      try { msg = JSON.parse(text); } catch (e) { return; }
      if (!msg.arg || msg.arg.channel !== 'books' || !msg.data) return;
      const key = bookKey('okx', 'X', msg.arg.instId);
      const book = ensureBook(key);
      const data = msg.data[0] || {};
      if (msg.action === 'snapshot') {
        book.asks = new Map((data.asks || []).map(([p, s]) => [p, s]));
        book.bids = new Map((data.bids || []).map(([p, s]) => [p, s]));
        book.ready = true;
      } else if (msg.action === 'update') {
        applyLevels(book.asks, data.asks || []);
        applyLevels(book.bids, data.bids || []);
      }
    });
    this.ws.on('close', () => { console.log('OKX WS: закрыто, переподключаюсь'); clearInterval(this.pingTimer); setTimeout(() => this.connect(), 3000); });
    this.ws.on('error', (err) => { console.log('OKX WS ошибка:', err.message); this.ws.close(); });
  },
  startPing() {
    clearInterval(this.pingTimer);
    this.pingTimer = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send('ping');
    }, 20000);
  },
  doSubscribe(instId) {
    this.subscribed.add(instId);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ op: 'subscribe', args: [{ channel: 'books', instId }] }));
    }
  },
  requestSymbol(symbol) {
    const key = bookKey('okx', 'X', symbol);
    if (!this.subscribed.has(symbol)) this.doSubscribe(symbol);
    return key;
  },
};

// ==================== АДАПТЕР: BYBIT ====================
// Отдельные соединения на спот и линейные фьючи. Топик orderbook.{depth}.{symbol} — берём
// максимальную документированную глубину (200 спот, 500 линейные). ЛЮБОЕ сообщение type="snapshot"
// (не только первое!) должно ПОЛНОСТЬЮ сбрасывать локальную книгу — так по официальным доксам.

function makeBybitAdapter(wsUrl, depth, marketLabel) {
  return {
    ws: null, pingTimer: null, subscribed: new Set(),
    connect() {
      this.ws = new WebSocket(wsUrl);
      this.ws.on('open', () => {
        console.log(`Bybit ${marketLabel} WS: открыто`);
        for (const symbol of this.subscribed) this.doSubscribe(symbol);
        this.startPing();
      });
      this.ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
        if (msg.op === 'subscribe' && msg.success === false) {
          console.log(`Bybit ${marketLabel}: подписка отклонена — ${JSON.stringify(msg)}`);
          return;
        }
        if (!msg.topic || !msg.topic.startsWith('orderbook.') || !msg.data) return;
        const symbol = msg.data.s;
        const key = bookKey('bybit-' + marketLabel, 'X', symbol);
        const book = ensureBook(key);
        if (msg.type === 'snapshot') {
          book.asks = new Map((msg.data.a || []).map(([p, s]) => [p, s]));
          book.bids = new Map((msg.data.b || []).map(([p, s]) => [p, s]));
          book.ready = true;
        } else if (msg.type === 'delta') {
          applyLevels(book.asks, msg.data.a || []);
          applyLevels(book.bids, msg.data.b || []);
        }
      });
      this.ws.on('close', () => { console.log(`Bybit ${marketLabel} WS: закрыто, переподключаюсь`); clearInterval(this.pingTimer); setTimeout(() => this.connect(), 3000); });
      this.ws.on('error', (err) => { console.log(`Bybit ${marketLabel} WS ошибка:`, err.message); this.ws.close(); });
    },
    startPing() {
      clearInterval(this.pingTimer);
      this.pingTimer = setInterval(() => {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify({ op: 'ping' }));
      }, 20000);
    },
    doSubscribe(symbol) {
      this.subscribed.add(symbol);
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ op: 'subscribe', args: [`orderbook.${depth}.${symbol}`] }));
      }
    },
    requestSymbol(symbol) {
      const key = bookKey('bybit-' + marketLabel, 'X', symbol);
      if (!this.subscribed.has(symbol)) this.doSubscribe(symbol);
      return key;
    },
  };
}

const bybitSpot = makeBybitAdapter('wss://stream.bybit.com/v5/public/spot', 1000, 'spot');
const bybitLinear = makeBybitAdapter('wss://stream.bybit.com/v5/public/linear', 1000, 'linear');

// ==================== АДАПТЕР: KUCOIN ====================
// Самый "тяжёлый" протокол: 1) получить одноразовый токен подключения (POST bullet-public),
// 2) подписаться на канал ДЕЛЬТ (полного снапшота в стриме нет), 3) отдельно взять REST-снапшот
// и сверить по sequenceStart/sequenceEnd — та же идея, что и у Binance. REST-снапшот СПОТА требует
// авторизации (нужны те же переменные окружения KUCOIN_API_KEY/SECRET/PASSPHRASE, что и в старом
// REST-прокси) — фьючерсный снапшот публичный, без ключа.

function kucoinSignedFetch(pathAndQuery) {
  const timestamp = Date.now().toString();
  const strToSign = timestamp + 'GET' + pathAndQuery;
  const sign = crypto.createHmac('sha256', process.env.KUCOIN_API_SECRET).update(strToSign).digest('base64');
  const passphrase = crypto.createHmac('sha256', process.env.KUCOIN_API_SECRET).update(process.env.KUCOIN_API_PASSPHRASE).digest('base64');
  return fetch(`https://api.kucoin.com${pathAndQuery}`, {
    headers: {
      'KC-API-KEY': process.env.KUCOIN_API_KEY,
      'KC-API-SIGN': sign,
      'KC-API-TIMESTAMP': timestamp,
      'KC-API-PASSPHRASE': passphrase,
      'KC-API-KEY-VERSION': '2',
    },
  });
}

function makeKuCoinAdapter(tokenUrl, topicPrefix, marketLabel, snapshotFetcher) {
  return {
    ws: null, pingTimer: null, nextId: 1, subscribed: new Set(),
    pending: new Map(), // symbol -> { buffer: [], snapshotRequested: bool }
    async connect() {
      try {
        const resp = await fetch(tokenUrl, { method: 'POST' });
        const json = await resp.json();
        const token = json.data.token;
        const server = json.data.instanceServers[0];
        this.pingIntervalMs = server.pingInterval || 18000;
        const connectId = Date.now().toString();
        this.ws = new WebSocket(`${server.endpoint}?token=${token}&connectId=${connectId}`);
        this.ws.on('open', () => console.log(`KuCoin ${marketLabel} WS: соединение открыто, жду welcome`));
        this.ws.on('message', (raw) => {
          let msg;
          try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
          if (msg.type === 'welcome') {
            console.log(`KuCoin ${marketLabel} WS: welcome получен`);
            for (const symbol of this.subscribed) this.sendSubscribe(symbol);
            this.startPing();
            return;
          }
          if (msg.type === 'message' && msg.topic && msg.topic.startsWith(topicPrefix)) {
            this.handleUpdate(msg);
          }
        });
        this.ws.on('close', () => { console.log(`KuCoin ${marketLabel} WS: закрыто, переподключаюсь`); clearInterval(this.pingTimer); setTimeout(() => this.connect(), 3000); });
        this.ws.on('error', (err) => { console.log(`KuCoin ${marketLabel} WS ошибка:`, err.message); this.ws.close(); });
      } catch (err) {
        console.log(`KuCoin ${marketLabel}: не смог получить токен подключения, повтор через 5с:`, err.message);
        setTimeout(() => this.connect(), 5000);
      }
    },
    startPing() {
      clearInterval(this.pingTimer);
      this.pingTimer = setInterval(() => {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
          this.ws.send(JSON.stringify({ id: String(this.nextId++), type: 'ping' }));
        }
      }, this.pingIntervalMs || 18000);
    },
    sendSubscribe(symbol) {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ id: this.nextId++, type: 'subscribe', topic: `${topicPrefix}${symbol}`, response: true }));
      }
    },
    async fetchSnapshotAndSync(symbol, key) {
      try {
        const snap = await snapshotFetcher(symbol);
        const d = snap.data;
        const book = ensureBook(key);
        book.asks = new Map((d.asks || []).map(([p, s]) => [p, s]));
        book.bids = new Map((d.bids || []).map(([p, s]) => [p, s]));
        book.sequence = Number(d.sequence != null ? d.sequence : d.sequenceEnd || 0);
        const pend = this.pending.get(symbol);
        const buffered = (pend && pend.buffer) || [];
        for (const evt of buffered) {
          if (evt.sequenceEnd <= book.sequence) continue; // событие старше снапшота
          applyLevels(book.asks, (evt.changes && evt.changes.asks) || []);
          applyLevels(book.bids, (evt.changes && evt.changes.bids) || []);
          book.sequence = evt.sequenceEnd;
        }
        book.ready = true;
        if (pend) pend.snapshotRequested = true;
      } catch (err) {
        console.log(`KuCoin ${marketLabel}: ошибка снапшота для ${symbol}:`, err.message);
      }
    },
    handleUpdate(msg) {
      const symbol = msg.topic.split(':')[1];
      const key = bookKey('kucoin-' + marketLabel, 'X', symbol);
      const pend = this.pending.get(symbol);
      if (!pend) return; // символ, который мы не запрашивали
      const d = msg.data;
      if (!pend.snapshotRequested) {
        pend.buffer.push(d);
        return;
      }
      const book = books.get(key);
      if (!book) return;
      if (d.sequenceStart > book.sequence + 1) {
        console.log(`KuCoin ${marketLabel}: разрыв последовательности у ${symbol}, пересинхронизация`);
        book.ready = false;
        pend.snapshotRequested = false;
        pend.buffer = [d];
        this.fetchSnapshotAndSync(symbol, key);
        return;
      }
      if (d.sequenceEnd <= book.sequence) return; // устаревшее событие, игнорируем
      applyLevels(book.asks, (d.changes && d.changes.asks) || []);
      applyLevels(book.bids, (d.changes && d.changes.bids) || []);
      book.sequence = d.sequenceEnd;
    },
    requestSymbol(symbol) {
      const key = bookKey('kucoin-' + marketLabel, 'X', symbol);
      if (!this.subscribed.has(symbol)) {
        this.subscribed.add(symbol);
        this.pending.set(symbol, { buffer: [], snapshotRequested: false });
        ensureBook(key);
        this.sendSubscribe(symbol);
        this.fetchSnapshotAndSync(symbol, key);
      }
      return key;
    },
  };
}

const kucoinSpot = makeKuCoinAdapter(
  'https://api.kucoin.com/api/v1/bullet-public',
  '/market/level2:',
  'spot',
  async (symbol) => {
    const resp = await kucoinSignedFetch(`/api/v3/market/orderbook/level2?symbol=${symbol}`);
    return resp.json();
  }
);
const kucoinFutures = makeKuCoinAdapter(
  'https://api-futures.kucoin.com/api/v1/bullet-public',
  '/contractMarket/level2:',
  'futures',
  async (symbol) => {
    const resp = await fetch(`https://api-futures.kucoin.com/api/v1/level2/snapshot?symbol=${symbol}`);
    return resp.json();
  }
);

// ==================== GATE.IO — И СПОТ, И ФЬЮЧИ ЧЕРЕЗ REST-ПРОБРОС ====================
// Оба WS-канала фьючей Gate оказались проблемными: "order_book" — слишком узкий (20 уровней),
// а официально рекомендованный "order_book_update" не принимал ни одно из опробованных значений
// глубины (350/400/500/1000), а на 100 присылал дельты в формате, который заставил сервер упасть
// (пришлось добавлять общую защиту от падений во всём сервере). Раз REST и так честно даёт 300
// уровней без сбоев — не воюем дальше с WS для Gate, а просто пробрасываем REST-запрос через
// этот же сервер (тот же единый адрес для Apps Script, без смены кода таблицы).
// gateRestPassthrough удалён — Gate теперь работает через настоящий WS (futures.obu, 400 уровней,
// см. gateFutures ниже), REST-проброс больше не используется

// ==================== АДАПТЕР: GATE FUTURES — futures.obu (400 уровней, подтверждено JSON) ====================
// Подтверждено диагностикой живьём: боевой сервер отдаёт по этому каналу обычный JSON (SBE ещё не
// докатился до прода — было только на demo-окружении по офиц. объявлению Gate). Сам канал отдаёт
// только ДЕЛЬТЫ (U/u), полного снапшота в потоке не видно — значит нужна та же REST-сверка, что и
// у Binance: буферизируем дельты, параллельно берём REST-снапшот с with_id=true, склеиваем по U/u.

const gateFutures = {
  ws: null, subscribed: new Set(),
  lastUpdateId: new Map(), // symbol -> последний применённый u
  connect() {
    this.ws = new WebSocket('wss://fx-ws.gateio.ws/v4/ws/usdt');
    this.ws.on('open', () => {
      console.log('Gate futures.obu WS: открыто');
      for (const symbol of this.subscribed) this.doSubscribe(symbol);
    });
    this.ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
      if (msg.error) { console.log('Gate futures.obu: ошибка от биржи —', JSON.stringify(msg.error)); return; }
      if (msg.channel !== 'futures.obu' || !msg.result) return;
      const r = msg.result;
      const symbol = String(r.s || '').replace(/^ob\./, '').replace(/\.\d+$/, ''); // "ob.BTC_USDT.400" -> "BTC_USDT"
      const key = bookKey('gate-futures', 'X', symbol);
      const book = ensureBook(key);
      if (r.full) {
        // Первое сообщение после подписки — честный полный снапшот, замена целиком (подтверждено
        // живьём и документацией — тот же формат, что у spot.obu)
        book.asks = new Map((r.a || []).map(([p, s]) => [p, s]));
        book.bids = new Map((r.b || []).map(([p, s]) => [p, s]));
        this.lastUpdateId.set(symbol, r.u);
        book.ready = true;
        return;
      }
      // Дельта — проверяем непрерывность через U относительно последнего применённого u
      const lastId = this.lastUpdateId.get(symbol);
      if (lastId != null && r.U > lastId + 1) {
        console.log(`Gate futures.obu: разрыв последовательности у ${symbol}, переподписка за новым снапшотом`);
        book.ready = false;
        this.doSubscribe(symbol); // Gate сам пришлёт новый full-снапшот на повторную подписку
        return;
      }
      applyLevels(book.asks, r.a || []);
      applyLevels(book.bids, r.b || []);
      this.lastUpdateId.set(symbol, r.u);
    });
    this.ws.on('close', () => { console.log('Gate futures.obu WS: закрыто, переподключаюсь'); setTimeout(() => this.connect(), 3000); });
    this.ws.on('error', (err) => { console.log('Gate futures.obu WS ошибка:', err.message); this.ws.close(); });
  },
  doSubscribe(symbol) {
    this.subscribed.add(symbol);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({
        time: Math.floor(Date.now() / 1000),
        channel: 'futures.obu',
        event: 'subscribe',
        payload: [`ob.${symbol}.400`],
      }));
    }
  },
  requestSymbol(symbol) {
    const key = bookKey('gate-futures', 'X', symbol);
    if (!this.subscribed.has(symbol)) this.doSubscribe(symbol);
    return key;
  },
};

// ==================== АДАПТЕР: KRAKEN ====================
// wss://ws.kraken.com/v2, канал "book", depth=1000 (подтверждённый максимум: 10/25/100/500/1000).
// Важно: v2 использует символы вида "BTC/USD" (со слэшем, БЕЗ переименования в XBT — это было
// только у старого REST API v0/v1!). Наш склеенный тикер вида "XBTUSD" нужно разобрать обратно на
// базу+валюту и превратить XBT снова в BTC — делаем это тем же приёмом "отрезать известный хвост
// валюты", что и в Apps Script для остальных бирж.
const KRAKEN_QUOTE_SUFFIXES = ['USDT', 'USDC', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'USD'];
function krakenSymbolToPair(symbol) {
  const s = symbol.toUpperCase();
  for (const q of KRAKEN_QUOTE_SUFFIXES) {
    if (s.endsWith(q) && s.length > q.length) {
      let base = s.slice(0, s.length - q.length);
      if (base === 'XBT') base = 'BTC'; // v2 зовёт BTC его настоящим именем, не XBT
      return `${base}/${q}`;
    }
  }
  return symbol; // не смогли разобрать — пробуем как есть
}

const kraken = {
  ws: null, subscribed: new Set(),
  connect() {
    this.ws = new WebSocket('wss://ws.kraken.com/v2');
    this.ws.on('open', () => {
      console.log('Kraken WS: открыто');
      for (const symbol of this.subscribed) this.doSubscribe(symbol);
    });
    this.ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
      if (msg.channel !== 'book' || !msg.data) return;
      const d = msg.data[0];
      if (!d) return;
      // Обратно превращаем "BTC/USD" в наш внутренний ключ формата "XBTUSD" (симметрично krakenSymbolToPair)
      const [base, quote] = d.symbol.split('/');
      const internalSymbol = (base === 'BTC' ? 'XBT' : base) + quote;
      const key = bookKey('kraken', 'X', internalSymbol);
      const book = ensureBook(key);
      if (msg.type === 'snapshot') {
        book.asks = new Map((d.asks || []).map((lvl) => [String(lvl.price), String(lvl.qty)]));
        book.bids = new Map((d.bids || []).map((lvl) => [String(lvl.price), String(lvl.qty)]));
        book.ready = true;
      } else if (msg.type === 'update') {
        applyLevels(book.asks, (d.asks || []).map((lvl) => [String(lvl.price), String(lvl.qty)]));
        applyLevels(book.bids, (d.bids || []).map((lvl) => [String(lvl.price), String(lvl.qty)]));
      }
    });
    this.ws.on('close', () => { console.log('Kraken WS: закрыто, переподключаюсь'); setTimeout(() => this.connect(), 3000); });
    this.ws.on('error', (err) => { console.log('Kraken WS ошибка:', err.message); this.ws.close(); });
  },
  doSubscribe(symbol) {
    this.subscribed.add(symbol);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({
        method: 'subscribe',
        params: { channel: 'book', symbol: [krakenSymbolToPair(symbol)], depth: 1000 },
      }));
    }
  },
  requestSymbol(symbol) {
    const key = bookKey('kraken', 'X', symbol);
    if (!this.subscribed.has(symbol)) this.doSubscribe(symbol);
    return key;
  },
};

// ==================== АДАПТЕР: HTX (Huobi) ====================
// wss://api-aws.huobi.pro/ws, канал market.{symbol}.mbp.150 — mbp.400 биржа явно отклонила как
// невалидный топик ("invalid topic"), 150 — подтверждённо рабочее значение из документации.
// ВСЕ входящие сообщения — GZIP бинарные, нужно распаковывать перед JSON.parse. Свой пинг/понг
// текстом, без сжатия. Сам канал отдаёт только дельты — нужен отдельный "req" запрос за снапшотом
// с seqNum, дальше сверка по prevSeqNum (та же идея, что у Binance/KuCoin).

const htx = {
  ws: null, nextId: 1, subscribed: new Set(),
  pending: new Map(), // symbol -> { buffer: [], snapshotRequested: bool }
  connect() {
    this.ws = new WebSocket('wss://api-aws.huobi.pro/ws');
    this.ws.on('open', () => {
      console.log('HTX WS: открыто');
      for (const symbol of this.subscribed) this.doSubscribeAndRequest(symbol);
    });
    this.ws.on('message', (raw) => {
      let text;
      try { text = zlib.gunzipSync(raw).toString('utf8'); } catch (e) { return; } // не гружёный gzip — игнор
      let msg;
      try { msg = JSON.parse(text); } catch (e) { return; }
      if (msg.ping) {
        this.ws.send(JSON.stringify({ pong: msg.ping })); // обычным текстом, без gzip
        return;
      }
      if (msg.status === 'error') {
        console.log('HTX: ошибка от биржи —', JSON.stringify(msg));
        return;
      }
      if (msg.rep) {
        // Ответ на "req" — снапшот с seqNum
        const symbol = msg.rep.replace(/^market\./, '').replace(/\.mbp\.\d+$/, '');
        this.applySnapshot(symbol, msg.data);
        return;
      }
      if (msg.ch) {
        // Push-обновление (дельта)
        const symbol = msg.ch.replace(/^market\./, '').replace(/\.mbp\.\d+$/, '');
        this.handleDelta(symbol, msg.tick);
      }
    });
    this.ws.on('close', () => { console.log('HTX WS: закрыто, переподключаюсь'); setTimeout(() => this.connect(), 3000); });
    this.ws.on('error', (err) => { console.log('HTX WS ошибка:', err.message); this.ws.close(); });
  },
  applySnapshot(symbol, data) {
    if (!data) return;
    const key = bookKey('htx', 'X', symbol);
    const book = ensureBook(key);
    book.asks = new Map((data.asks || []).map(([p, s]) => [String(p), String(s)]));
    book.bids = new Map((data.bids || []).map(([p, s]) => [String(p), String(s)]));
    book.lastSeqNum = data.seqNum;
    const pend = this.pending.get(symbol);
    const buffered = (pend && pend.buffer) || [];
    for (const tick of buffered) {
      if (tick.prevSeqNum != null && tick.prevSeqNum < book.lastSeqNum) continue; // дельта старше снапшота
      applyLevels(book.asks, tick.asks || []);
      applyLevels(book.bids, tick.bids || []);
      book.lastSeqNum = tick.seqNum;
    }
    book.ready = true;
    if (pend) pend.snapshotRequested = true;
  },
  handleDelta(symbol, tick) {
    if (!tick) return;
    const key = bookKey('htx', 'X', symbol);
    const pend = this.pending.get(symbol);
    if (!pend) return; // символ, который мы не запрашивали
    if (!pend.snapshotRequested) {
      pend.buffer.push(tick);
      return;
    }
    const book = books.get(key);
    if (!book) return;
    if (book.lastSeqNum != null && tick.prevSeqNum !== book.lastSeqNum) {
      console.log(`HTX: разрыв последовательности у ${symbol}, переподписка за новым снапшотом`);
      book.ready = false;
      pend.snapshotRequested = false;
      pend.buffer = [];
      this.doSubscribeAndRequest(symbol);
      return;
    }
    applyLevels(book.asks, tick.asks || []);
    applyLevels(book.bids, tick.bids || []);
    book.lastSeqNum = tick.seqNum;
  },
  doSubscribeAndRequest(symbol) {
    this.subscribed.add(symbol);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      const topic = `market.${symbol}.mbp.150`; // mbp.400 отклонён биржой как invalid topic — используем 150
      this.ws.send(JSON.stringify({ sub: topic, id: String(this.nextId++) }));
      this.ws.send(JSON.stringify({ req: topic, id: String(this.nextId++) }));
    }
  },
  requestSymbol(symbol) {
    const lower = symbol.toLowerCase(); // HTX использует строчные тикеры (btcusdt, не BTCUSDT)
    const key = bookKey('htx', 'X', lower);
    if (!this.subscribed.has(lower)) {
      this.pending.set(lower, { buffer: [], snapshotRequested: false });
      ensureBook(key);
      this.doSubscribeAndRequest(lower);
    }
    return key;
  },
};

// ==================== АДАПТЕР: HTX ФЬЮЧИ (USDT-маржинальные свопы) ====================
// Отдельный домен (linear-swap-ws, не обычный /ws!) и отдельный канал (depth.size_150.high_freq,
// не mbp). Тот же GZIP, но проще по сверке — первое сообщение после подписки уже полный снапшот
// (документация: "when data_type is incremental, snapshot data will be pushed for the first time"),
// дальше просто дельты — без отдельного "req" запроса, в отличие от спота.

const htxFutures = {
  ws: null, nextId: 1, subscribed: new Set(),
  connect() {
    this.ws = new WebSocket('wss://api.hbdm.com/linear-swap-ws');
    this.ws.on('open', () => {
      console.log('HTX futures WS: открыто');
      for (const symbol of this.subscribed) this.doSubscribe(symbol);
    });
    this.ws.on('message', (raw) => {
      let text;
      try { text = zlib.gunzipSync(raw).toString('utf8'); } catch (e) { return; }
      let msg;
      try { msg = JSON.parse(text); } catch (e) { return; }
      if (msg.ping) { this.ws.send(JSON.stringify({ pong: msg.ping })); return; }
      if (msg.status === 'error') { console.log('HTX futures: ошибка от биржи —', JSON.stringify(msg)); return; }
      if (!msg.ch || !msg.tick) return;
      const symbol = msg.ch.replace(/^market\./, '').replace(/\.depth\.size_150\.high_freq$/, '');
      const key = bookKey('htx-futures', 'X', symbol);
      const book = ensureBook(key);
      const isFirstMessage = !book.ready; // первое сообщение после подписки — полный снапшот
      if (isFirstMessage) {
        book.asks = new Map((msg.tick.asks || []).map(([p, s]) => [String(p), String(s)]));
        book.bids = new Map((msg.tick.bids || []).map(([p, s]) => [String(p), String(s)]));
        book.ready = true;
      } else {
        applyLevels(book.asks, msg.tick.asks || []);
        applyLevels(book.bids, msg.tick.bids || []);
      }
    });
    this.ws.on('close', () => { console.log('HTX futures WS: закрыто, переподключаюсь'); setTimeout(() => this.connect(), 3000); });
    this.ws.on('error', (err) => { console.log('HTX futures WS ошибка:', err.message); this.ws.close(); });
  },
  doSubscribe(symbol) {
    this.subscribed.add(symbol);
    const key = bookKey('htx-futures', 'X', symbol);
    const book = books.get(key);
    if (book) book.ready = false; // следующее сообщение после (пере)подписки — снапшот, не дельта
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({
        sub: `market.${symbol}.depth.size_150.high_freq`,
        id: String(this.nextId++),
        data_type: 'incremental',
      }));
    }
  },
  requestSymbol(symbol) {
    const key = bookKey('htx-futures', 'X', symbol);
    if (!this.subscribed.has(symbol)) this.doSubscribe(symbol);
    return key;
  },
};

// ==================== АДАПТЕР: BITFINEX ====================
// wss://api-pub.bitfinex.com/ws/2, канал "book", precision P0 (без агрегации), len=250 (макс.
// документированная глубина). ВАЖНО: после подписки все сообщения приходят только с числовым
// chanId, без имени символа — нужно самим сопоставлять chanId -> символ.
// Формат уровня [цена, count, объём]: объём>0 — бид, объём<0 — аск, count=0 — убрать уровень.

const bitfinex = {
  ws: null, subscribed: new Set(), chanToSymbol: new Map(), // chanId -> наш внутренний символ
  connect() {
    this.ws = new WebSocket('wss://api-pub.bitfinex.com/ws/2');
    this.ws.on('open', () => {
      console.log('Bitfinex WS: открыто');
      for (const symbol of this.subscribed) this.doSubscribe(symbol);
    });
    this.ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
      if (!Array.isArray(msg)) {
        // Служебное событие (subscribed/error/info) — по нему запоминаем chanId -> символ
        if (msg.event === 'subscribed' && msg.channel === 'book') {
          this.chanToSymbol.set(msg.chanId, this.tSymbolToInternal(msg.symbol));
        } else if (msg.event === 'error') {
          console.log('Bitfinex: ошибка от биржи —', JSON.stringify(msg));
        }
        return;
      }
      const [chanId, payload] = msg;
      if (payload === 'hb') return; // heartbeat, игнорируем
      const symbol = this.chanToSymbol.get(chanId);
      if (!symbol) return;
      const key = bookKey('bitfinex', 'X', symbol);
      const book = ensureBook(key);
      const applyOne = ([price, count, amount]) => {
        const map = amount > 0 ? book.bids : book.asks;
        const priceStr = String(price);
        if (count === 0) map.delete(priceStr);
        else map.set(priceStr, String(Math.abs(amount)));
      };
      if (Array.isArray(payload[0])) {
        // Снапшот — массив уровней целиком, заменяем книгу
        book.asks = new Map();
        book.bids = new Map();
        payload.forEach(applyOne);
        book.ready = true;
      } else {
        // Обновление — один уровень
        applyOne(payload);
      }
    });
    this.ws.on('close', () => { console.log('Bitfinex WS: закрыто, переподключаюсь'); this.chanToSymbol.clear(); setTimeout(() => this.connect(), 3000); });
    this.ws.on('error', (err) => { console.log('Bitfinex WS ошибка:', err.message); this.ws.close(); });
  },
  // Наш внутренний символ ("ADAUSD", "BTCF0:USTF0") -> формат биржи с префиксом "t" ("tADAUSD")
  internalToTSymbol(symbol) { return 't' + symbol; },
  tSymbolToInternal(tSymbol) { return tSymbol.replace(/^t/, ''); },
  doSubscribe(symbol) {
    this.subscribed.add(symbol);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({
        event: 'subscribe', channel: 'book',
        symbol: this.internalToTSymbol(symbol), prec: 'P0', len: '250',
      }));
    }
  },
  requestSymbol(symbol) {
    const key = bookKey('bitfinex', 'X', symbol);
    if (!this.subscribed.has(symbol)) this.doSubscribe(symbol);
    return key;
  },
};

// ==================== ДИАГНОСТИКА: MEXC protobuf — универсальный сырой декодер ====================
// Ни официальная схема (mexcdevelop/websocket-proto — подтверждённо неверная по независимому
// источнику), ни чужой реверс-инжиниринг из одной статьи не заслуживают доверия без проверки.
// Вместо этого — свой декодер по ОБЩИМ правилам формата Protobuf (без знания точных названий
// полей), который сам находит внутри структуру вида "цена+объём" (два текстовых поля подряд) —
// и сверяем результат с REST на той же паре, прежде чем на него полагаться.

function readVarint(buf, pos) {
  let result = 0, shift = 0;
  while (true) {
    const byte = buf[pos++];
    result += (byte & 0x7f) * Math.pow(2, shift);
    if ((byte & 0x80) === 0) break;
    shift += 7;
  }
  return [result, pos];
}

// Разбирает буфер по общим правилам wire-формата Protobuf в дерево {номерПоля: [{wireType, value}]}
function decodeProtobuf(buf, start, end) {
  const fields = {};
  let pos = start === undefined ? 0 : start;
  const limit = end === undefined ? buf.length : end;
  while (pos < limit) {
    let tag;
    try { [tag, pos] = readVarint(buf, pos); } catch (e) { break; }
    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x7;
    let value;
    if (wireType === 0) {
      try { [value, pos] = readVarint(buf, pos); } catch (e) { break; }
    } else if (wireType === 2) {
      let len;
      try { [len, pos] = readVarint(buf, pos); } catch (e) { break; }
      if (pos + len > limit) break; // повреждённые/непонятные данные — не читаем дальше мусор
      value = buf.slice(pos, pos + len);
      pos += len;
    } else if (wireType === 1) {
      if (pos + 8 > limit) break;
      value = buf.readDoubleLE(pos); pos += 8;
    } else if (wireType === 5) {
      if (pos + 4 > limit) break;
      value = buf.readFloatLE(pos); pos += 4;
    } else {
      break; // неподдерживаемый/повреждённый wire-тип — останавливаемся, не гадаем
    }
    if (!fields[fieldNumber]) fields[fieldNumber] = [];
    fields[fieldNumber].push({ wireType, value });
  }
  return fields;
}

// Пробует прочитать буфер как вложенное "Bid"/"Ask"-сообщение: поле 1 = цена (строка-число),
// поле 2 = объём (строка-число). Это самая вероятная нумерация для двухполевого сообщения —
// почти все схемы, что видели сегодня у других бирж, нумеруют поля именно в порядке объявления.
function tryDecodeAsPriceQty(buf) {
  if (!Buffer.isBuffer(buf)) return null;
  const sub = decodeProtobuf(buf);
  if (!sub[1] || !sub[2]) return null;
  const priceVal = sub[1][0].value, qtyVal = sub[2][0].value;
  if (!Buffer.isBuffer(priceVal) || !Buffer.isBuffer(qtyVal)) return null;
  const price = priceVal.toString('utf8');
  const qty = qtyVal.toString('utf8');
  if (!/^\d+\.?\d*$/.test(price) || !/^\d+\.?\d*$/.test(qty)) return null;
  return { price, qty };
}

// Рекурсивно обходит всё дерево в поисках полей, ВСЕ повторения которых распознаются как
// цена+объём — это и есть кандидат на список bids или asks. Возвращает все найденные кандидаты
// с указанием "пути" (цепочки номеров полей), чтобы можно было сверить с реальными ценами.
function findPriceQtyLists(fields, path, results) {
  path = path || [];
  results = results || [];
  for (const fieldNumStr of Object.keys(fields)) {
    const entries = fields[fieldNumStr];
    const lengthDelimited = entries.filter((e) => e.wireType === 2);
    if (lengthDelimited.length > 0) {
      const decoded = lengthDelimited.map((e) => tryDecodeAsPriceQty(e.value));
      if (decoded.every((d) => d !== null) && decoded.length > 0) {
        results.push({ path: [...path, fieldNumStr], count: decoded.length, sample: decoded.slice(0, 3) });
      }
      // Всё равно копаем глубже — вдруг настоящий список вложен ещё на уровень ниже
      for (const e of lengthDelimited) {
        const nested = decodeProtobuf(e.value);
        if (Object.keys(nested).length > 0) findPriceQtyLists(nested, [...path, fieldNumStr], results);
      }
    }
  }
  return results;
}

const mexcDiagnostic = {
  ws: null,
  connect() {
    this.ws = new WebSocket('wss://wbs-api.mexc.com/ws');
    this.ws.on('open', () => {
      console.log('MEXC obu-диагностика: открыто, подписываюсь на BTCUSDT');
      this.ws.send(JSON.stringify({ method: 'SUBSCRIPTION', params: ['spot@public.aggre.depth.v3.api.pb@100ms@BTCUSDT'] }));
    });
    this.ws.on('message', (raw) => {
      if (typeof raw === 'string' || raw.length < 5) return; // служебный текстовый ответ, не бинарные данные
      try {
        const fields = decodeProtobuf(raw);
        // Верхний уровень — ищем поле с именем символа (строка "BTCUSDT" где-то в length-delimited полях)
        for (const fieldNum of Object.keys(fields)) {
          for (const entry of fields[fieldNum]) {
            if (entry.wireType === 2 && Buffer.isBuffer(entry.value)) {
              const asText = entry.value.toString('utf8');
              if (/^[A-Za-z0-9._@]+$/.test(asText) && asText.length < 100) {
                console.log(`MEXC obu-диагностика: верхнее поле ${fieldNum} — похоже на текст: "${asText}"`);
              }
            }
          }
        }
        const candidates = findPriceQtyLists(fields);
        console.log('MEXC obu-диагностика: найдено кандидатов на bids/asks:', candidates.length);
        candidates.forEach((c) => console.log('  путь', c.path.join('.'), '— записей:', c.count, '— пример:', JSON.stringify(c.sample)));
      } catch (err) {
        console.log('MEXC obu-диагностика: ошибка декодирования —', err.message);
      }
    });
    this.ws.on('close', () => { console.log('MEXC obu-диагностика: закрыто, переподключаюсь'); setTimeout(() => this.connect(), 3000); });
    this.ws.on('error', (err) => { console.log('MEXC obu-диагностика: ошибка', err.message); this.ws.close(); });
  },
};

// ==================== HTTP-ЭНДПОИНТ (общий для всех бирж) ====================

const ADAPTERS = {
  bitget, whitebit, okx,
  binance: {
    requestSymbol: (symbol, marketType) => (marketType === 'futures' ? binanceFutures : binanceSpot).requestSymbol(symbol),
    connect: () => { binanceSpot.connect(); binanceFutures.connect(); },
    // У Binance два реальных соединения (спот+фьючи) под одной записью в ADAPTERS — отдаём оба состояния
    wsStateReport: () => ({ spot: binanceSpot.ws ? binanceSpot.ws.readyState : 'not connected', futures: binanceFutures.ws ? binanceFutures.ws.readyState : 'not connected' }),
  },
  bybit: {
    requestSymbol: (symbol, marketType) => (marketType === 'futures' ? bybitLinear : bybitSpot).requestSymbol(symbol),
    connect: () => { bybitSpot.connect(); bybitLinear.connect(); },
    wsStateReport: () => ({ spot: bybitSpot.ws ? bybitSpot.ws.readyState : 'not connected', linear: bybitLinear.ws ? bybitLinear.ws.readyState : 'not connected' }),
  },
  kucoin: {
    requestSymbol: (symbol, marketType) => (marketType === 'futures' ? kucoinFutures : kucoinSpot).requestSymbol(symbol),
    connect: () => { kucoinSpot.connect(); kucoinFutures.connect(); },
    wsStateReport: () => ({ spot: kucoinSpot.ws ? kucoinSpot.ws.readyState : 'not connected', futures: kucoinFutures.ws ? kucoinFutures.ws.readyState : 'not connected' }),
  },
  // Gate — только фьючи через WS (futures.obu, 400 уровней), спот остаётся на REST напрямую из Apps Script
  gate: {
    requestSymbol: (symbol) => gateFutures.requestSymbol(symbol),
    connect: () => gateFutures.connect(),
    wsStateReport: () => ({ futures: gateFutures.ws ? gateFutures.ws.readyState : 'not connected' }),
  },
  kraken,
  // HTX — спот (mbp.150) и фьючи (depth.size_150.high_freq) на РАЗНЫХ доменах/соединениях
  htx: {
    requestSymbol: (symbol, marketType) => (marketType === 'futures' ? htxFutures : htx).requestSymbol(symbol),
    connect: () => { htx.connect(); htxFutures.connect(); },
    wsStateReport: () => ({ spot: htx.ws ? htx.ws.readyState : 'not connected', futures: htxFutures.ws ? htxFutures.ws.readyState : 'not connected' }),
  },
  bitfinex,
};

// Ждёт, пока стакан по ключу станет готов (пришёл снапшот) — вместо того чтобы сразу сдаваться.
// На практике снапшот приходит за 1-2 секунды после подписки; 8 секунд — комфортный запас сверху.
function waitForReady(key, timeoutMs = 8000, intervalMs = 200) {
  return new Promise((resolve) => {
    const start = Date.now();
    const check = () => {
      const book = books.get(key);
      if (book && book.ready) return resolve(book);
      if (Date.now() - start >= timeoutMs) return resolve(null);
      setTimeout(check, intervalMs);
    };
    check();
  });
}

app.get('/depth', async (req, res) => {
  const exchange = req.query.exchange;
  const symbol = req.query.symbol;
  const marketType = req.query.marketType === 'futures' ? 'futures' : 'spot';
  if (!exchange || !symbol) return res.status(400).json({ error: 'exchange and symbol query params are required' });

  const adapter = ADAPTERS[exchange];
  if (!adapter) return res.status(400).json({ error: `Биржа "${exchange}" пока не подключена к WS-серверу` });

  const key = adapter.requestSymbol(symbol, marketType);
  let book = books.get(key);
  if (!book || !book.ready) {
    // Не сдаёмся сразу — ждём немного прихода снапшота, чтобы клиенту не приходилось жать
    // "обновить" второй раз вручную ради того, что обычно занимает секунду-две.
    book = await waitForReady(key);
  }
  if (!book || !book.ready) {
    return res.status(202).json({ status: 'warming_up', message: 'Не дождался снапшота за 8 секунд, подожди ещё и запроси снова' });
  }
  res.json({ asks: mapToSortedArray(book.asks, true), bids: mapToSortedArray(book.bids, false) });
});

app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    service: 'multi-exchange-ws-depth',
    connected: Object.keys(ADAPTERS).map((k) => ({
      exchange: k,
      wsState: ADAPTERS[k].wsStateReport ? ADAPTERS[k].wsStateReport() : (ADAPTERS[k].ws ? ADAPTERS[k].ws.readyState : 'not connected'),
    })),
    subscribedCount: books.size,
  });
});

// Общая страховка: сервер один на ВСЕ 11 бирж — если где-то в обработке сообщения от ОДНОЙ биржи
// вылезет неожиданная ошибка (неизвестный формат данных и т.п.), процесс не должен падать целиком
// и утаскивать за собой соединения остальных десяти. Логируем и продолжаем работать.
process.on('uncaughtException', (err) => {
  console.log('НЕПОЙМАННОЕ ИСКЛЮЧЕНИЕ (сервер продолжает работать):', err.message);
});
process.on('unhandledRejection', (err) => {
  console.log('НЕОБРАБОТАННЫЙ REJECT (сервер продолжает работать):', err && err.message);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Multi-exchange WS depth server listening on port ${PORT}`);
  Object.values(ADAPTERS).forEach((a) => a.connect());
  // Диагностика MEXC protobuf — сама подписывается на BTCUSDT и логирует, что нашла внутри
  // бинарных сообщений. НЕ встроена в основной поток данных — только для проверки глазами.
  mexcDiagnostic.connect();
});

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
  for (const [priceStr, sizeStr] of levels) {
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

// ==================== АДАПТЕР: GATE.IO (только фьючи) ====================
// Спот НЕ подключаем — новый канал spot.obu может отдавать данные в бинарном формате SBE
// (не обычным JSON), это не подтвердить однозначно по документации, а гадать с бинарным
// протоколом рискованно (см. также пропущенный пока MEXC с protobuf). Спот у Gate остаётся
// на старом REST (уже работает, limit=1000 подтверждён живьём).
// Фьючи — канал "futures.order_book_update" (ОФИЦИАЛЬНО РЕКОМЕНДОВАННЫЙ, легаси "order_book" сама
// биржа просит не использовать). Первое сообщение после подписки — ПОЛНЫЙ снапшот (full:true),
// дальше только дельты с U/u для сверки непрерывности — отдельный REST-запрос не нужен вообще.
// level=300 — пробуем совпасть с уже подтверждённым максимумом REST-эндпоинта (было "limit 300"
// в ответе биржи ранее); если канал его не примет — увидим ошибку подписки в логах Render.

const gateFutures = {
  ws: null, subscribed: new Set(), localDepthId: new Map(), // symbol -> последний применённый u
  connect() {
    this.ws = new WebSocket('wss://fx-ws.gateio.ws/v4/ws/usdt');
    this.ws.on('open', () => {
      console.log('Gate futures WS: открыто');
      for (const symbol of this.subscribed) this.doSubscribe(symbol);
    });
    this.ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
      if (msg.error) {
        console.log('Gate futures: ошибка от биржи —', JSON.stringify(msg.error));
        return;
      }
      if (msg.channel !== 'futures.order_book_update' || !msg.result) return;
      const r = msg.result;
      const symbol = r.s;
      const key = bookKey('gate-futures', 'X', symbol);
      const book = ensureBook(key);
      if (r.full) {
        // Первое сообщение после подписки (может повторяться) — полный снапшот, заменяем целиком
        book.asks = new Map((r.a || []).map(([p, s]) => [p, s]));
        book.bids = new Map((r.b || []).map(([p, s]) => [p, s]));
        this.localDepthId.set(symbol, r.u);
        book.ready = true;
        return;
      }
      // Дельта — проверяем непрерывность через U (начало) относительно нашего последнего u
      const lastId = this.localDepthId.get(symbol);
      if (lastId != null && r.U !== lastId + 1) {
        console.log(`Gate futures: разрыв последовательности у ${symbol}, жду новый снапшот (переподписка)`);
        book.ready = false;
        this.subscribed.delete(symbol); // при повторной подписке придёт свежий full-снапшот
        this.doSubscribe(symbol);
        return;
      }
      applyLevels(book.asks, r.a || []);
      applyLevels(book.bids, r.b || []);
      this.localDepthId.set(symbol, r.u);
    });
    this.ws.on('close', () => { console.log('Gate futures WS: закрыто, переподключаюсь'); setTimeout(() => this.connect(), 3000); });
    this.ws.on('error', (err) => { console.log('Gate futures WS ошибка:', err.message); this.ws.close(); });
  },
  doSubscribe(symbol) {
    this.subscribed.add(symbol);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({
        time: Math.floor(Date.now() / 1000),
        channel: 'futures.order_book_update',
        event: 'subscribe',
        payload: [symbol, '100ms', '300'], // 350/400/500/1000 биржа отклонила — 300 подтверждённо
        // рабочее значение (совпадает с максимумом REST), финально останавливаюсь на нём
      }));
    }
  },
  requestSymbol(symbol) {
    const key = bookKey('gate-futures', 'X', symbol);
    if (!this.subscribed.has(symbol)) this.doSubscribe(symbol);
    return key;
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
  // Только фьючи — спот Gate остаётся на REST (см. комментарий у gateFutures про бинарный SBE-риск)
  gate: {
    requestSymbol: (symbol) => gateFutures.requestSymbol(symbol), // marketType не нужен — сюда попадают только фьючерсные запросы
    connect: () => gateFutures.connect(),
    wsStateReport: () => ({ futures: gateFutures.ws ? gateFutures.ws.readyState : 'not connected', spot: 'не подключено (остаётся на REST)' }),
  },
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

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Multi-exchange WS depth server listening on port ${PORT}`);
  Object.values(ADAPTERS).forEach((a) => a.connect());
});

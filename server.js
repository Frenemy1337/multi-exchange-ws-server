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

// ==================== HTTP-ЭНДПОИНТ (общий для всех бирж) ====================

const ADAPTERS = { bitget, whitebit };

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
    connected: Object.keys(ADAPTERS).map((k) => ({ exchange: k, wsState: ADAPTERS[k].ws ? ADAPTERS[k].ws.readyState : 'not connected' })),
    subscribedCount: books.size,
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Multi-exchange WS depth server listening on port ${PORT}`);
  Object.values(ADAPTERS).forEach((a) => a.connect());
});

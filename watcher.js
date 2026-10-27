/**
 * TJ VIEW — 24/7 Cloud Alert Watcher & Telegram Push Daemon
 * ────────────────────────────────────────────────────────
 * Runs as a standalone background worker on any cloud host (Render, Railway, Fly.io, or VPS)
 * Continuously connects to TradingView Cloud WebSocket to monitor price alerts in Turso DB
 * and dispatches instant Telegram push notifications to your phone when prices hit.
 */

import fs from 'fs';
import path from 'path';
import http from 'http';
import WebSocket from 'ws';

// Auto-load .env file without external dependencies
try {
  const possiblePaths = [
    path.resolve(process.cwd(), '.env'),
    path.resolve(process.cwd(), 'cloud-worker', '.env'),
  ];
  for (const envPath of possiblePaths) {
    if (fs.existsSync(envPath)) {
      const content = fs.readFileSync(envPath, 'utf8');
      content.split(/\r?\n/).forEach((line) => {
        const trimmed = line.trim();
        if (trimmed && !trimmed.startsWith('#')) {
          const eq = trimmed.indexOf('=');
          if (eq > 0) {
            const key = trimmed.slice(0, eq).trim();
            const val = trimmed.slice(eq + 1).trim();
            if (!process.env[key]) process.env[key] = val;
          }
        }
      });
      break;
    }
  }
} catch {}

// Configuration
const TURSO_URL = process.env.TURSO_DATABASE_URL || '';
const TURSO_TOKEN = process.env.TURSO_AUTH_TOKEN || '';
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || '15000', 10);
const PORT = process.env.PORT || 3000;

if (!TURSO_URL || !TURSO_TOKEN) {
  console.warn('⚠️ [TJ Cloud Watcher] TURSO_DATABASE_URL or TURSO_AUTH_TOKEN not set.');
  console.warn('   Please configure environment variables or create a .env file.');
}

if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
  console.info('ℹ️ [TJ Cloud Watcher] Telegram credentials not set. Mobile notifications disabled.');
  console.info('   To enable Telegram alerts, provide TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.');
}

// Data structures
const activeAlerts = new Map();
const previousPrices = new Map();
const activeTickers = new Set();

let ws = null;
let sessionId = `qs_cloud_${Math.random().toString(36).slice(2, 8)}`;
let isReady = false;
let isReconnecting = false;

// ── Turso LibSQL HTTP Helper ──
async function executeTurso(sql, args = []) {
  if (!TURSO_URL || !TURSO_TOKEN) return null;
  try {
    const cleanUrl = TURSO_URL.replace(/^libsql:\/\//, 'https://');
    const endpoint = `${cleanUrl}/v2/pipeline`;

    const res = await fetch(endpoint, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${TURSO_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        requests: [
          {
            type: 'execute',
            stmt: {
              sql,
              args: args.map((val) => {
                if (typeof val === 'number') return { type: 'float', value: val };
                if (typeof val === 'string') return { type: 'text', value: val };
                if (val === null || val === undefined) return { type: 'null' };
                return { type: 'text', value: String(val) };
              }),
            },
          },
          { type: 'close' },
        ],
      }),
    });

    if (res.ok) {
      const data = await res.json();
      return data?.results?.[0]?.response?.result;
    } else {
      const txt = await res.text();
      console.warn(`[Turso Error ${res.status}]`, txt);
    }
  } catch (err) {
    console.warn('[Turso Request Failed]:', err.message);
  }
  return null;
}

// ── Telegram Notification Dispatcher ──
async function sendTelegramAlert(alert, currentPrice) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;

  const conditionEmoji = alert.condition === 'ABOVE' ? '📈' : alert.condition === 'BELOW' ? '📉' : '⇄';
  const conditionLabel = alert.condition === 'ABOVE' ? 'Crossed Above (≥)' : alert.condition === 'BELOW' ? 'Crossed Below (≤)' : 'Crossed';

  const message = [
    `🚨 <b>TJ VIEW — PRICE ALERT TRIGGERED!</b>`,
    `━━━━━━━━━━━━━━━━━━━━`,
    `🔔 <b>Asset:</b> <code>${alert.symbol}</code>`,
    `🎯 <b>Target:</b> <code>${Number(alert.targetPrice).toLocaleString()}</code>`,
    `⚡ <b>Trigger Price:</b> <code>${Number(currentPrice).toLocaleString()}</code>`,
    `${conditionEmoji} <b>Condition:</b> ${conditionLabel}`,
    `⏱ <b>Time:</b> ${new Date().toLocaleString('en-GB', { timeZone: 'Asia/Kolkata' })} IST`,
    `━━━━━━━━━━━━━━━━━━━━`,
    `<i>TJ VIEW Autonomous 24/7 Cloud Watcher</i>`,
  ].join('\n');

  try {
    const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text: message,
        parse_mode: 'HTML',
        disable_web_page_preview: true,
      }),
    });

    if (!res.ok) {
      console.warn(`[Telegram Error ${res.status}]:`, await res.text());
    } else {
      console.log(`📱 [Telegram] Push notification successfully delivered for ${alert.symbol}!`);
    }
  } catch (err) {
    console.warn('[Telegram Dispatch Failed]:', err.message);
  }
}

// ── Mark Alert Triggered in Turso ──
async function markAlertTriggered(alertId) {
  const now = Date.now();
  await executeTurso(
    'UPDATE price_alerts SET triggered = 1, triggered_at = ? WHERE id = ?;',
    [now, alertId]
  );
}

// ── WebSocket Send Framing ──
function sendWs(m, p) {
  const msg = JSON.stringify({ m, p });
  const framed = `~m~${msg.length}~m~${msg}`;
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(framed);
  }
}

// ── Poll Turso for Active Alerts ──
async function pollTursoAlerts() {
  if (!TURSO_URL || !TURSO_TOKEN) return;

  const result = await executeTurso(
    'SELECT id, symbol, ticker, target_price, condition FROM price_alerts WHERE triggered = 0;'
  );

  if (!result || !Array.isArray(result.rows)) return;

  const getVal = (col) => (col && typeof col === 'object' && 'value' in col ? col.value : col);
  const currentDbIds = new Set();
  const neededTickers = new Set();

  for (const row of result.rows) {
    const id = String(getVal(row[0]));
    const symbol = String(getVal(row[1]));
    const ticker = String(getVal(row[2]));
    const targetPrice = Number(getVal(row[3]));
    const condition = getVal(row[4]) || 'ABOVE';

    currentDbIds.add(id);
    neededTickers.add(ticker);

    if (!activeAlerts.has(id)) {
      activeAlerts.set(id, { id, symbol, ticker, targetPrice, condition });
      console.log(`➕ [CloudWatcher] Loaded alert: ${symbol} (${ticker}) ${condition} ${targetPrice}`);
    }
  }

  // Remove deleted / already handled alerts
  for (const id of activeAlerts.keys()) {
    if (!currentDbIds.has(id)) {
      activeAlerts.delete(id);
    }
  }

  // Dynamic symbol sync on WebSocket
  if (isReady && ws && ws.readyState === WebSocket.OPEN) {
    const toAdd = [];
    const toRemove = [];

    for (const t of neededTickers) {
      if (!activeTickers.has(t)) {
        toAdd.push(t);
        activeTickers.add(t);
      }
    }

    for (const t of activeTickers) {
      if (!neededTickers.has(t)) {
        toRemove.push(t);
        activeTickers.delete(t);
      }
    }

    if (toRemove.length > 0) {
      sendWs('quote_remove_symbols', [sessionId, ...toRemove]);
    }
    if (toAdd.length > 0) {
      console.log(`⚡ [CloudWatcher] Subscribing new symbols to live feed:`, toAdd);
      sendWs('quote_add_symbols', [sessionId, ...toAdd]);
    }
  }
}

// ── Handle Incoming Price Quotes ──
function handleQuoteUpdate(ticker, price) {
  const prevPrice = previousPrices.get(ticker);

  for (const [id, alert] of activeAlerts.entries()) {
    if (alert.ticker !== ticker) continue;

    let isHit = false;
    if (alert.condition === 'ABOVE') {
      isHit = price >= alert.targetPrice;
    } else if (alert.condition === 'BELOW') {
      isHit = price <= alert.targetPrice;
    } else if (alert.condition === 'CROSSING') {
      if (prevPrice !== undefined) {
        const up = prevPrice < alert.targetPrice && price >= alert.targetPrice;
        const down = prevPrice > alert.targetPrice && price <= alert.targetPrice;
        isHit = up || down;
      } else {
        isHit = Math.abs(price - alert.targetPrice) / alert.targetPrice < 0.0005;
      }
    }

    if (isHit) {
      console.log(`\n🚨🚨 [TRIGGER HIT] ${alert.symbol} reached ${price}! Target was: ${alert.targetPrice} (${alert.condition})`);
      activeAlerts.delete(id);

      // Async DB mark & Telegram dispatch
      markAlertTriggered(id).catch(console.error);
      sendTelegramAlert(alert, price).catch(console.error);
    }
  }

  previousPrices.set(ticker, price);
}

// ── TradingView Cloud WebSocket Client ──
function connectWebSocket() {
  if (isReconnecting) return;
  sessionId = `qs_cloud_${Math.random().toString(36).slice(2, 8)}`;
  isReady = false;

  console.log('⚡ [CloudWatcher] Connecting to TradingView Cloud WebSocket...');

  ws = new WebSocket('wss://data.tradingview.com/socket.io/websocket', {
    headers: {
      Origin: 'https://www.tradingview.com',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    },
  });

  ws.on('open', () => {
    console.log('✅ [CloudWatcher] WebSocket Connected to TradingView!');
  });

  ws.on('message', (data) => {
    const text = data.toString();

    // Heartbeat ping-pong
    if (text.startsWith('~h~')) {
      ws?.send(text);
      return;
    }

    // Split framed messages
    const parts = text.split(/~m~\d+~m~/).filter(Boolean);
    for (const part of parts) {
      try {
        const d = JSON.parse(part);

        // Handshake packet
        if (d.session_id) {
          console.log('🤝 [CloudWatcher] Handshake confirmed. Initializing quote session:', sessionId);
          isReady = true;

          sendWs('set_auth_token', ['unauthorized_user_token']);
          sendWs('quote_create_session', [sessionId]);
          sendWs('quote_set_fields', [sessionId, 'lp', 'ch', 'chp']);

          // Subscribe all current active tickers
          const tickers = Array.from(activeTickers);
          if (tickers.length > 0) {
            console.log(`⚡ [CloudWatcher] Subscribing initial symbols:`, tickers);
            sendWs('quote_add_symbols', [sessionId, ...tickers]);
          }
        }

        // Real-Time Quote update: qsd
        if (d.m === 'qsd' && d.p && d.p[1]) {
          const item = d.p[1];
          const sym = item.n;
          const vals = item.v;

          if (sym && vals && typeof vals.lp === 'number') {
            handleQuoteUpdate(sym, vals.lp);
          }
        }
      } catch {}
    }
  });

  ws.on('close', () => {
    console.warn('⚠️ [CloudWatcher] WebSocket disconnected! Reconnecting in 3s...');
    scheduleReconnect();
  });

  ws.on('error', (err) => {
    console.error('❌ [CloudWatcher] WebSocket error:', err.message);
  });
}

function scheduleReconnect() {
  if (isReconnecting) return;
  isReconnecting = true;
  isReady = false;
  setTimeout(() => {
    isReconnecting = false;
    connectWebSocket();
  }, 3000);
}

// ── Startup Flow ──
async function main() {
  console.log('═══════════════════════════════════════════════════════');
  console.log('🚀 TJ VIEW Autonomous 24/7 Cloud Alert Watcher Started');
  console.log('═══════════════════════════════════════════════════════');

  // HTTP Health Check Endpoint (Allows free 24/7 hosting on Render Web Service, Koyeb, Railway)
  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(
      JSON.stringify({
        status: 'ONLINE',
        service: 'TJ VIEW 24/7 Cloud Alert Watcher',
        activeAlerts: activeAlerts.size,
        monitoredSymbols: Array.from(activeTickers),
        websocketReady: isReady,
        uptimeSeconds: Math.floor(process.uptime()),
        timestamp: new Date().toISOString(),
      }, null, 2)
    );
  });

  server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
      const fallbackPort = Number(PORT) + 10;
      server.listen(fallbackPort, () => {
        console.log(`🌐 [CloudWatcher] Health check server listening on fallback port ${fallbackPort}`);
      });
    } else {
      console.warn('Server error:', err.message);
    }
  });

  server.listen(PORT, () => {
    console.log(`🌐 [CloudWatcher] Health check server listening on port ${PORT}`);
  });

  // Initial poll to populate alerts
  await pollTursoAlerts();

  // Connect WebSocket
  connectWebSocket();

  // Periodic poll to fetch new alerts added via web app
  setInterval(pollTursoAlerts, POLL_INTERVAL_MS);
}

main().catch(console.error);

// Clean exit
process.on('SIGINT', () => {
  console.log('\n🛑 Shutting down Cloud Watcher...');
  ws?.close();
  process.exit(0);
});
process.on('SIGTERM', () => {
  console.log('\n🛑 Shutting down Cloud Watcher...');
  ws?.close();
  process.exit(0);
});

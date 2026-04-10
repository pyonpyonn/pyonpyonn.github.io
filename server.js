// Live Reload Server — no npm install needed
const http = require('http');
const fs = require('fs');
const path = require('path');
const net = require('net');

const PORT = 8080;
const ROOT = __dirname;

// ── MIME types ──────────────────────────────────────────────────────────────
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css':  'text/css',
  '.js':   'application/javascript',
  '.json': 'application/json',
  '.png':  'image/png',
  '.jpg':  'image/jpeg',
  '.svg':  'image/svg+xml',
  '.ico':  'image/x-icon',
  '.woff2':'font/woff2',
  '.woff': 'font/woff',
  '.ttf':  'font/ttf',
};

// ── WebSocket clients waiting for reload signal ──────────────────────────────
const clients = new Set();

// ── Tiny WebSocket handshake + broadcast ────────────────────────────────────
function wsHandshake(req, socket) {
  const key = req.headers['sec-websocket-key'];
  const accept = require('crypto')
    .createHash('sha1')
    .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
    .digest('base64');
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\nConnection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );
  clients.add(socket);
  socket.on('close', () => clients.delete(socket));
  socket.on('error', () => clients.delete(socket));
}

function broadcast() {
  // WebSocket text frame: fin=1, opcode=1, no mask, payload "reload"
  const payload = Buffer.from('reload');
  const frame = Buffer.alloc(2 + payload.length);
  frame[0] = 0x81;
  frame[1] = payload.length;
  payload.copy(frame, 2);
  for (const s of clients) { try { s.write(frame); } catch(_) {} }
  console.log(`[reload] → ${clients.size} client(s)`);
}

// ── Inject live-reload snippet into HTML ─────────────────────────────────────
const SNIPPET = `<script>
(function(){
  var ws=new WebSocket('ws://127.0.0.1:${PORT}/__livereload');
  ws.onmessage=function(e){if(e.data==='reload')location.reload();};
  ws.onclose=function(){setTimeout(function(){location.reload();},1500);};
})();
</script>`;

// ── Yahoo Finance proxy helpers ───────────────────────────────────────────────
const https = require('https');
function httpsGet(url) {
  return new Promise((resolve, reject) => {
    const opts = Object.assign(require('url').parse(url), {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36',
        'Accept': 'application/json',
      },
      timeout: 10000,
    });
    const req = https.get(opts, res => {
      let buf = '';
      res.on('data', d => buf += d);
      res.on('end', () => resolve({ status: res.statusCode, body: buf }));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

function sendJSON(res, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
  res.end(body);
}

// Fetch a Yahoo Finance URL — race direct + 2 proxies, return first 200
function yahooFetch(yaUrl) {
  const attempt = url => httpsGet(url).then(r => {
    if (r.status === 200) return r.body;
    throw new Error('HTTP ' + r.status);
  });
  return Promise.any([
    attempt(yaUrl),
    attempt('https://api.allorigins.win/raw?url=' + encodeURIComponent(yaUrl)),
    attempt('https://corsproxy.io/?' + encodeURIComponent(yaUrl)),
  ]).catch(() => { throw new Error('All Yahoo sources failed'); });
}

async function handleQuoteAPI(req, res) {
  const qs = require('url').parse(req.url, true).query;
  const symbols = (qs.symbols || '').split(',').map(s => s.trim()).filter(Boolean);
  if (!symbols.length) return sendJSON(res, { ok: false, error: 'No symbols' });

  const quotes = {};
  // Fetch each symbol via the chart endpoint (meta has price, prev close, high, low)
  // Use parallel requests, cap concurrency at 6
  const chunks = [];
  for (let i = 0; i < symbols.length; i += 6) chunks.push(symbols.slice(i, i + 6));

  for (const chunk of chunks) {
    await Promise.allSettled(chunk.map(async sym => {
      const yaUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?interval=1d&range=5d&includePrePost=false`;
      try {
        const body = await yahooFetch(yaUrl);
        const json = JSON.parse(body);
        const meta = json.chart && json.chart.result && json.chart.result[0] && json.chart.result[0].meta;
        if (!meta || !meta.regularMarketPrice) throw new Error('no meta');
        quotes[sym] = {
          price:     meta.regularMarketPrice,
          prevClose: meta.previousClose || meta.chartPreviousClose || meta.regularMarketPrice,
          change:    meta.regularMarketPrice - (meta.previousClose || meta.chartPreviousClose || meta.regularMarketPrice),
          changePct: (meta.previousClose || meta.chartPreviousClose)
            ? ((meta.regularMarketPrice - (meta.previousClose || meta.chartPreviousClose)) / (meta.previousClose || meta.chartPreviousClose) * 100)
            : 0,
          high:      meta.regularMarketDayHigh  || meta.regularMarketPrice,
          low:       meta.regularMarketDayLow   || meta.regularMarketPrice,
          currency:  meta.currency || 'USD',
          name:      meta.shortName || sym,
          source:    'Yahoo',
        };
      } catch (e) {
        // symbol failed silently — caller will use sim data
      }
    }));
  }

  if (!Object.keys(quotes).length) return sendJSON(res, { ok: false, error: 'No data from Yahoo' });
  sendJSON(res, { ok: true, quotes });
}

async function handleChartAPI(req, res) {
  const qs = require('url').parse(req.url, true).query;
  const symbol   = qs.symbol   || '';
  const range    = qs.range    || '1d';
  const interval = qs.interval || '5m';
  if (!symbol) return sendJSON(res, { ok: false, error: 'No symbol' });

  const yaUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=${interval}&range=${range}&includePrePost=false`;

  try {
    const body = await yahooFetch(yaUrl);
    const json = JSON.parse(body);
    const result = json.chart && json.chart.result && json.chart.result[0];
    if (!result || !result.timestamp) throw new Error('No data');

    const ts = result.timestamp;
    const q  = result.indicators.quote[0];
    const meta = result.meta || {};
    const candles = [];
    for (let i = 0; i < ts.length; i++) {
      if (q.close[i] == null) continue;
      candles.push({
        time:   ts[i],
        open:   q.open[i]   || q.close[i],
        high:   q.high[i]   || q.close[i],
        low:    q.low[i]    || q.close[i],
        close:  q.close[i],
        volume: q.volume[i] || 0,
      });
    }
    sendJSON(res, {
      ok: true, candles,
      meta: {
        regularMarketPrice:   meta.regularMarketPrice,
        previousClose:        meta.previousClose || meta.chartPreviousClose,
        regularMarketDayHigh: meta.regularMarketDayHigh,
        regularMarketDayLow:  meta.regularMarketDayLow,
        currency:             meta.currency,
      }
    });
  } catch (e) {
    console.warn('[/api/chart] Error:', e.message);
    sendJSON(res, { ok: false, error: e.message });
  }
}

// ── HTTP server ──────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  const urlPath = req.url.split('?')[0];

  // API routes
  if (urlPath === '/api/quote') return handleQuoteAPI(req, res);
  if (urlPath === '/api/chart') return handleChartAPI(req, res);

  let filePath = path.join(ROOT, urlPath === '/' ? 'index.html' : urlPath);

  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    const ext = path.extname(filePath).toLowerCase();
    const mime = MIME[ext] || 'application/octet-stream';
    res.writeHead(200, { 'Content-Type': mime });
    if (ext === '.html') {
      // inject snippet before </body>
      const html = data.toString().replace('</body>', SNIPPET + '\n</body>');
      res.end(html);
    } else {
      res.end(data);
    }
  });
});

// ── Upgrade HTTP → WebSocket for /__livereload ───────────────────────────────
server.on('upgrade', (req, socket) => {
  if (req.url === '/__livereload') wsHandshake(req, socket);
  else socket.destroy();
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`\n  Live server ready → http://127.0.0.1:${PORT}/`);
  console.log('  Watching for file changes...\n');
});

// ── File watcher ─────────────────────────────────────────────────────────────
let debounce = null;
fs.watch(ROOT, { recursive: true }, (event, filename) => {
  if (!filename) return;
  if (filename.includes('node_modules') || filename.includes('.git')) return;
  clearTimeout(debounce);
  debounce = setTimeout(() => {
    console.log(`[changed] ${filename}`);
    broadcast();
  }, 150);
});

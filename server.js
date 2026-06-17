'use strict';
const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
const axios = require('axios');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use('/assets', express.static(__dirname));

const contacts = require('./contacts');
const TOTAL = contacts.length;

// ── Config ────────────────────────────────────────────────────────────────────
let cfg = {
  baseUrl: '',
  apiKey: '',
  instanceName: 'fastescova',
  webhookUrl: ''
};

// ── Dispatch state ────────────────────────────────────────────────────────────
let state = {
  running: false,
  paused: false,
  currentIndex: 0,
  sent: 0,
  errors: 0,
  skipped: 0,
  statuses: new Array(TOTAL).fill('pending'),   // pending|sending|sent|error|skipped
  timestamps: new Array(TOTAL).fill(''),
  errorMsgs: new Array(TOTAL).fill(''),
  // Engagement tracking
  classifications: new Array(TOTAL).fill('none'), // none|frio|morno|quente
  readTimes: new Array(TOTAL).fill(''),
  replyTimes: new Array(TOTAL).fill(''),
  replies: new Array(TOTAL).fill(''),
  messageIds: new Array(TOTAL).fill(''),
  hot: 0,
  warm: 0,
  cold: 0,
  // Settings — delays in MINUTES
  message: 'Olá {nome}! Temos novidades incríveis na Fast Escova esperando por você! 💛✨',
  minDelay: 1,
  maxDelay: 3,
  nextIn: 0,
  _dispatchTimer: null,
  _countdownTimer: null
};

// ── Phone lookup maps ─────────────────────────────────────────────────────────
const phoneToIndex = {};
contacts.forEach((c, i) => {
  const digits = c.phone.replace(/\D/g, '');
  if (digits.length < 8) return;
  phoneToIndex[digits] = i;
  if (digits.startsWith('55') && digits.length >= 12) {
    phoneToIndex[digits.slice(2)] = i;
  } else if (!digits.startsWith('55') && digits.length >= 10) {
    phoneToIndex['55' + digits] = i;
  }
});

const msgIdToIndex = {};

// ── Helpers ───────────────────────────────────────────────────────────────────
function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(c => { if (c.readyState === 1) c.send(msg); });
}

function formatPhone(raw) {
  const digits = raw.replace(/\D/g, '');
  if (digits.length < 8) return null;
  if (/^0+$/.test(digits) || /^1+$/.test(digits)) return null;
  if (digits.startsWith('000')) return null;
  if (digits.startsWith('55') && digits.length >= 12) return digits;
  return '55' + digits;
}

async function evoReq(method, endpoint, data) {
  const url = cfg.baseUrl.replace(/\/$/, '') + '/' + endpoint.replace(/^\//, '');
  const res = await axios({
    method, url, data,
    headers: { apikey: cfg.apiKey, 'Content-Type': 'application/json' },
    timeout: 20000
  });
  return res.data;
}

function now() {
  return new Date().toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function normalizeEvent(raw) {
  if (!raw) return '';
  return raw.toLowerCase().replace(/_/g, '.').replace(/\s/g, '');
}

// ── Webhook endpoint ──────────────────────────────────────────────────────────
app.post('/webhook', (req, res) => {
  res.sendStatus(200);
  try {
    const body = req.body;
    const event = normalizeEvent(body.event || body.type || '');
    const data = body.data;

    // Message read receipts
    if (event === 'messages.update') {
      const updates = Array.isArray(data) ? data : (data ? [data] : []);
      updates.forEach(upd => {
        const msgId = upd?.key?.id;
        const status = upd?.update?.status ?? upd?.status;
        const idx = msgIdToIndex[msgId];
        if (idx === undefined) return;

        const isRead = status === 4 || status === 'READ';
        if (isRead && state.classifications[idx] !== 'quente') {
          const prev = state.classifications[idx];
          if (prev === 'morno') return; // already warm
          state.classifications[idx] = 'morno';
          state.readTimes[idx] = now();
          state.warm++;
          if (prev === 'frio') state.cold = Math.max(0, state.cold - 1);
          broadcast({ type: 'classification', index: idx, classification: 'morno', readTime: state.readTimes[idx], hot: state.hot, warm: state.warm, cold: state.cold });
          console.log(`🌡️  Morno [${idx + 1}] ${contacts[idx].name}`);
        }
      });
    }

    // Incoming message (reply)
    if (event === 'messages.upsert') {
      const msgs = Array.isArray(data?.messages) ? data.messages
                 : Array.isArray(data) ? data
                 : (data ? [data] : []);

      msgs.forEach(msg => {
        if (msg?.key?.fromMe === true) return;
        const remoteJid = msg?.key?.remoteJid || '';
        if (remoteJid.endsWith('@g.us')) return; // skip groups

        const jidPhone = remoteJid.split('@')[0];
        let idx = phoneToIndex[jidPhone];
        if (idx === undefined && jidPhone.startsWith('55')) {
          idx = phoneToIndex[jidPhone.slice(2)];
        }

        // Fallback: check if replying to one of our messages
        const quotedId = msg?.message?.extendedTextMessage?.contextInfo?.stanzaId
          || msg?.contextInfo?.stanzaId;
        if (idx === undefined && quotedId && msgIdToIndex[quotedId] !== undefined) {
          idx = msgIdToIndex[quotedId];
        }

        if (idx === undefined) return;

        const text = msg?.message?.conversation
          || msg?.message?.extendedTextMessage?.text
          || msg?.message?.imageMessage?.caption
          || '[mídia]';

        const prev = state.classifications[idx];
        if (prev === 'quente') {
          // Update reply text only
          state.replies[idx] = text.substring(0, 200);
          broadcast({ type: 'classification', index: idx, classification: 'quente', replyTime: state.replyTimes[idx], reply: state.replies[idx], hot: state.hot, warm: state.warm, cold: state.cold });
          return;
        }

        state.classifications[idx] = 'quente';
        state.replyTimes[idx] = now();
        state.replies[idx] = text.substring(0, 200);
        state.hot++;
        if (prev === 'morno') state.warm = Math.max(0, state.warm - 1);
        if (prev === 'frio') state.cold = Math.max(0, state.cold - 1);

        broadcast({ type: 'classification', index: idx, classification: 'quente', replyTime: state.replyTimes[idx], reply: state.replies[idx], hot: state.hot, warm: state.warm, cold: state.cold });
        console.log(`🔥 Quente [${idx + 1}] ${contacts[idx].name} — "${text.substring(0, 60)}"`);
      });
    }
  } catch (e) {
    console.error('Webhook error:', e.message);
  }
});

// ── Evolution API routes ──────────────────────────────────────────────────────
app.get('/api/config', (req, res) => {
  res.json({ baseUrl: cfg.baseUrl, apiKey: cfg.apiKey ? '***' : '', instanceName: cfg.instanceName, webhookUrl: cfg.webhookUrl });
});

app.post('/api/config', (req, res) => {
  const { baseUrl, apiKey, instanceName, webhookUrl } = req.body;
  if (baseUrl !== undefined) cfg.baseUrl = baseUrl.trim();
  if (apiKey && apiKey !== '***') cfg.apiKey = apiKey.trim();
  if (instanceName) cfg.instanceName = instanceName.trim();
  if (webhookUrl !== undefined) cfg.webhookUrl = webhookUrl.trim();
  res.json({ ok: true });
});

app.post('/api/instance/create', async (req, res) => {
  try {
    const d = await evoReq('post', '/instance/create', {
      instanceName: cfg.instanceName,
      qrcode: true,
      integration: 'WHATSAPP-BAILEYS'
    });
    res.json({ ok: true, data: d });
  } catch (e) {
    res.json({ ok: false, error: e.response?.data?.message || e.message });
  }
});

app.get('/api/instance/qr', async (req, res) => {
  try {
    const d = await evoReq('get', `/instance/connect/${cfg.instanceName}`);
    res.json({ ok: true, data: d });
  } catch (e) {
    res.json({ ok: false, error: e.response?.data?.message || e.message });
  }
});

app.post('/api/instance/pairing-code', async (req, res) => {
  try {
    const { phoneNumber } = req.body;
    const d = await evoReq('post', `/instance/pairingCode/${cfg.instanceName}`, { phoneNumber });
    res.json({ ok: true, data: d });
  } catch (e) {
    res.json({ ok: false, error: e.response?.data?.message || e.message });
  }
});

app.get('/api/instance/status', async (req, res) => {
  try {
    const d = await evoReq('get', `/instance/connectionState/${cfg.instanceName}`);
    res.json({ ok: true, data: d });
  } catch (e) {
    res.json({ ok: false, error: e.response?.data?.message || e.message });
  }
});

app.post('/api/webhook/configure', async (req, res) => {
  try {
    const { webhookUrl } = req.body;
    if (webhookUrl) cfg.webhookUrl = webhookUrl.trim();
    const target = cfg.webhookUrl;
    if (!target) return res.json({ ok: false, error: 'Informe a URL pública do servidor' });
    const webhookEndpoint = target.replace(/\/$/, '') + '/webhook';
    const d = await evoReq('post', `/webhook/set/${cfg.instanceName}`, {
      url: webhookEndpoint,
      events: ['MESSAGES_UPDATE', 'MESSAGES_UPSERT', 'CONNECTION_UPDATE'],
      webhook_by_events: false,
      webhook_base64: false
    });
    res.json({ ok: true, data: d, webhookUrl: webhookEndpoint });
  } catch (e) {
    res.json({ ok: false, error: e.response?.data?.message || e.message });
  }
});

// ── Contacts & Dispatch routes ────────────────────────────────────────────────
app.get('/api/contacts', (req, res) => {
  res.json(contacts.map((c, i) => ({
    ...c,
    status: state.statuses[i],
    timestamp: state.timestamps[i],
    errorMsg: state.errorMsgs[i],
    classification: state.classifications[i],
    readTime: state.readTimes[i],
    replyTime: state.replyTimes[i],
    reply: state.replies[i]
  })));
});

app.get('/api/dispatch/state', (req, res) => {
  res.json({
    running: state.running, paused: state.paused,
    currentIndex: state.currentIndex, sent: state.sent,
    errors: state.errors, skipped: state.skipped, total: TOTAL,
    message: state.message, minDelay: state.minDelay, maxDelay: state.maxDelay,
    nextIn: state.nextIn, hot: state.hot, warm: state.warm, cold: state.cold
  });
});

app.post('/api/dispatch/settings', (req, res) => {
  const { message, minDelay, maxDelay } = req.body;
  if (message !== undefined) state.message = message;
  if (minDelay !== undefined) state.minDelay = Math.max(0.5, parseFloat(minDelay));
  if (maxDelay !== undefined) state.maxDelay = Math.max(state.minDelay, parseFloat(maxDelay));
  res.json({ ok: true });
});

app.post('/api/dispatch/start', (req, res) => {
  if (state.running && !state.paused) return res.json({ ok: false, error: 'Já em execução' });
  const { message, minDelay, maxDelay } = req.body || {};
  if (message !== undefined) state.message = message;
  if (minDelay !== undefined) state.minDelay = Math.max(0.5, parseFloat(minDelay));
  if (maxDelay !== undefined) state.maxDelay = Math.max(state.minDelay, parseFloat(maxDelay));
  state.running = true;
  state.paused = false;
  res.json({ ok: true });
  scheduleNext();
});

app.post('/api/dispatch/pause', (req, res) => {
  state.paused = true;
  clearTimeout(state._dispatchTimer);
  clearInterval(state._countdownTimer);
  state.nextIn = 0;
  broadcast({ type: 'paused' });
  res.json({ ok: true });
});

app.post('/api/dispatch/reset', (req, res) => {
  clearTimeout(state._dispatchTimer);
  clearInterval(state._countdownTimer);
  state.running = false; state.paused = false;
  state.currentIndex = 0; state.sent = 0; state.errors = 0; state.skipped = 0;
  state.statuses = new Array(TOTAL).fill('pending');
  state.timestamps = new Array(TOTAL).fill('');
  state.errorMsgs = new Array(TOTAL).fill('');
  state.classifications = new Array(TOTAL).fill('none');
  state.readTimes = new Array(TOTAL).fill('');
  state.replyTimes = new Array(TOTAL).fill('');
  state.replies = new Array(TOTAL).fill('');
  state.messageIds = new Array(TOTAL).fill('');
  state.hot = 0; state.warm = 0; state.cold = 0; state.nextIn = 0;
  Object.keys(msgIdToIndex).forEach(k => delete msgIdToIndex[k]);
  broadcast({ type: 'reset' });
  res.json({ ok: true });
});

// ── Dispatch engine ───────────────────────────────────────────────────────────
async function sendOne(index) {
  const c = contacts[index];
  const phone = formatPhone(c.phone);

  if (!phone) {
    state.statuses[index] = 'skipped';
    state.timestamps[index] = now();
    state.errorMsgs[index] = 'Número inválido';
    state.skipped++;
    broadcast({ type: 'status', index, status: 'skipped', timestamp: state.timestamps[index], errorMsg: 'Número inválido' });
    return;
  }

  const text = state.message.replace(/\{nome\}/gi, c.name.split(' ')[0]);
  const resp = await evoReq('post', `/message/sendText/${cfg.instanceName}`, {
    number: phone, text, delay: 1200
  });

  // Track message for receipt callbacks
  const msgId = resp?.key?.id || resp?.id;
  if (msgId) {
    state.messageIds[index] = msgId;
    msgIdToIndex[msgId] = index;
  }
  phoneToIndex[phone] = index;
  if (phone.startsWith('55')) phoneToIndex[phone.slice(2)] = index;

  // Initially cold (sent but not read yet)
  state.classifications[index] = 'frio';
  state.cold++;
}

function scheduleNext() {
  if (state.paused || !state.running) return;

  if (state.currentIndex >= TOTAL) {
    state.running = false;
    broadcast({ type: 'complete', sent: state.sent, errors: state.errors, skipped: state.skipped });
    console.log(`✅ Disparo concluído — Enviados: ${state.sent} | Erros: ${state.errors} | Inválidos: ${state.skipped}`);
    return;
  }

  const idx = state.currentIndex;
  state.statuses[idx] = 'sending';
  broadcast({ type: 'status', index: idx, status: 'sending' });

  sendOne(idx)
    .then(() => {
      if (state.statuses[idx] === 'skipped') return; // handled inside sendOne
      state.statuses[idx] = 'sent';
      state.timestamps[idx] = now();
      state.sent++;
      broadcast({ type: 'status', index: idx, status: 'sent', timestamp: state.timestamps[idx], classification: state.classifications[idx] });
      console.log(`✓ [${idx + 1}/${TOTAL}] ${contacts[idx].name}`);
    })
    .catch(e => {
      state.statuses[idx] = 'error';
      state.timestamps[idx] = now();
      const errMsg = e.response?.data?.message || e.message || 'Erro';
      state.errorMsgs[idx] = errMsg;
      state.errors++;
      broadcast({ type: 'status', index: idx, status: 'error', timestamp: state.timestamps[idx], errorMsg: errMsg });
      console.error(`✗ [${idx + 1}/${TOTAL}] ${contacts[idx].name} — ${errMsg}`);
    })
    .finally(() => {
      broadcast({ type: 'stats', sent: state.sent, errors: state.errors, skipped: state.skipped, hot: state.hot, warm: state.warm, cold: state.cold });
      state.currentIndex++;

      if (state.paused || !state.running) return;
      if (state.currentIndex >= TOTAL) { scheduleNext(); return; }

      // Delay in minutes → convert to ms
      const minMs = state.minDelay * 60 * 1000;
      const maxMs = state.maxDelay * 60 * 1000;
      const delay = Math.round(minMs + Math.random() * (maxMs - minMs));
      const secs = Math.round(delay / 1000);
      state.nextIn = secs;

      clearInterval(state._countdownTimer);
      state._countdownTimer = setInterval(() => {
        if (state.nextIn > 0) state.nextIn--;
        broadcast({ type: 'countdown', seconds: state.nextIn });
      }, 1000);
      broadcast({ type: 'countdown', seconds: secs });

      state._dispatchTimer = setTimeout(() => {
        clearInterval(state._countdownTimer);
        state.nextIn = 0;
        scheduleNext();
      }, delay);
    });
}

// ── WebSocket ─────────────────────────────────────────────────────────────────
wss.on('connection', ws => {
  ws.send(JSON.stringify({
    type: 'init',
    total: TOTAL,
    statuses: state.statuses,
    timestamps: state.timestamps,
    errorMsgs: state.errorMsgs,
    classifications: state.classifications,
    readTimes: state.readTimes,
    replyTimes: state.replyTimes,
    replies: state.replies,
    sent: state.sent, errors: state.errors, skipped: state.skipped,
    hot: state.hot, warm: state.warm, cold: state.cold,
    currentIndex: state.currentIndex,
    running: state.running, paused: state.paused, nextIn: state.nextIn,
    message: state.message, minDelay: state.minDelay, maxDelay: state.maxDelay
  }));
});

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log('');
  console.log('  ╔══════════════════════════════════╗');
  console.log('  ║   Fast Escova — Painel Disparos  ║');
  console.log('  ╚══════════════════════════════════╝');
  console.log('');
  console.log(`  🚀  http://localhost:${PORT}`);
  console.log(`  🔗  Webhook → http://SEU-SERVIDOR:${PORT}/webhook`);
  console.log(`  👥  ${TOTAL} contatos carregados`);
  console.log('');
});

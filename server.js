'use strict';
const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
const axios = require('axios');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use('/assets', express.static(__dirname));

const contacts = require('./contacts');
const TOTAL = contacts.length;

// ── Supabase ──────────────────────────────────────────────────────────────────
const supabase = createClient(
  process.env.SUPABASE_URL  || 'https://gkbjemvwutaiksuueiqt.supabase.co',
  process.env.SUPABASE_KEY  || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdrYmplbXZ3dXRhaWtzdXVlaXF0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODAxMDg2OTQsImV4cCI6MjA5NTY4NDY5NH0.wZS9W4aLr68KEpkxHhHTakjibzRNxGAqgckz3n9VovQ'
);

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
  statuses: new Array(TOTAL).fill('pending'),
  timestamps: new Array(TOTAL).fill(''),
  errorMsgs: new Array(TOTAL).fill(''),
  classifications: new Array(TOTAL).fill('none'),
  readTimes: new Array(TOTAL).fill(''),
  replyTimes: new Array(TOTAL).fill(''),
  replies: new Array(TOTAL).fill(''),
  messageIds: new Array(TOTAL).fill(''),
  hot: 0, warm: 0, cold: 0,
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

// ── Supabase persistence ──────────────────────────────────────────────────────
async function loadFromDB() {
  try {
    // Config
    const { data: cfgRow } = await supabase.from('fast_config').select('*').eq('id', 1).single();
    if (cfgRow) {
      cfg.baseUrl      = cfgRow.base_url      || '';
      cfg.apiKey       = cfgRow.api_key       || '';
      cfg.instanceName = cfgRow.instance_name || 'fastescova';
      cfg.webhookUrl   = cfgRow.webhook_url   || '';
      if (cfgRow.message)    state.message   = cfgRow.message;
      if (cfgRow.min_delay)  state.minDelay  = parseFloat(cfgRow.min_delay);
      if (cfgRow.max_delay)  state.maxDelay  = parseFloat(cfgRow.max_delay);
    }

    // Run state
    const { data: run } = await supabase.from('fast_run_state').select('*').eq('id', 1).single();
    if (run) {
      state.currentIndex = run.current_index || 0;
      state.sent         = run.sent    || 0;
      state.errors       = run.errors  || 0;
      state.skipped      = run.skipped || 0;
      state.hot          = run.hot     || 0;
      state.warm         = run.warm    || 0;
      state.cold         = run.cold    || 0;
      // Never restore running/paused — always start fresh
    }

    // Dispatch rows
    const { data: rows } = await supabase.from('fast_dispatch').select('*');
    if (rows) {
      rows.forEach(r => {
        const i = r.idx;
        state.statuses[i]         = r.status         || 'pending';
        state.timestamps[i]       = r.sent_at        || '';
        state.classifications[i]  = r.classification  || 'none';
        state.readTimes[i]        = r.read_time      || '';
        state.replies[i]          = r.reply          || '';
        state.replyTimes[i]       = r.reply_time     || '';
        state.messageIds[i]       = r.message_id     || '';
        if (r.message_id) msgIdToIndex[r.message_id] = i;
      });
    }

    console.log('  ✅  Estado restaurado do Supabase');
  } catch (e) {
    console.warn('  ⚠️  Supabase load falhou, iniciando do zero:', e.message);
  }
}

async function saveConfig() {
  try {
    await supabase.from('fast_config').upsert({
      id: 1,
      base_url:      cfg.baseUrl,
      api_key:       cfg.apiKey,
      instance_name: cfg.instanceName,
      webhook_url:   cfg.webhookUrl,
      message:       state.message,
      min_delay:     state.minDelay,
      max_delay:     state.maxDelay,
      updated_at:    new Date().toISOString()
    });
  } catch (e) {
    console.warn('saveConfig error:', e.message);
  }
}

async function saveRunState() {
  try {
    await supabase.from('fast_run_state').upsert({
      id: 1,
      running:       state.running,
      paused:        state.paused,
      current_index: state.currentIndex,
      sent:          state.sent,
      errors:        state.errors,
      skipped:       state.skipped,
      hot:           state.hot,
      warm:          state.warm,
      cold:          state.cold,
      updated_at:    new Date().toISOString()
    });
  } catch (e) {
    console.warn('saveRunState error:', e.message);
  }
}

async function saveDispatch(i) {
  try {
    await supabase.from('fast_dispatch').upsert({
      idx:            i,
      status:         state.statuses[i]        || 'pending',
      sent_at:        state.timestamps[i]      || '',
      classification: state.classifications[i] || 'none',
      read_time:      state.readTimes[i]       || '',
      reply:          state.replies[i]         || '',
      reply_time:     state.replyTimes[i]      || '',
      message_id:     state.messageIds[i]      || '',
      updated_at:     new Date().toISOString()
    });
  } catch (e) {
    console.warn('saveDispatch error:', e.message);
  }
}

async function clearDispatchDB() {
  try {
    await supabase.from('fast_dispatch').delete().neq('idx', -1);
    await supabase.from('fast_run_state').upsert({
      id: 1, running: false, paused: false, current_index: 0,
      sent: 0, errors: 0, skipped: 0, hot: 0, warm: 0, cold: 0,
      updated_at: new Date().toISOString()
    });
  } catch (e) {
    console.warn('clearDispatch error:', e.message);
  }
}

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
          if (prev === 'morno') return;
          state.classifications[idx] = 'morno';
          state.readTimes[idx] = now();
          state.warm++;
          if (prev === 'frio') state.cold = Math.max(0, state.cold - 1);
          broadcast({ type: 'classification', index: idx, classification: 'morno', readTime: state.readTimes[idx], hot: state.hot, warm: state.warm, cold: state.cold });
          saveDispatch(idx);
          saveRunState();
          console.log(`🌡️  Morno [${idx + 1}] ${contacts[idx].name}`);
        }
      });
    }

    if (event === 'messages.upsert') {
      const msgs = Array.isArray(data?.messages) ? data.messages
                 : Array.isArray(data) ? data
                 : (data ? [data] : []);

      msgs.forEach(msg => {
        if (msg?.key?.fromMe === true) return;
        const remoteJid = msg?.key?.remoteJid || '';
        if (remoteJid.endsWith('@g.us')) return;

        const jidPhone = remoteJid.split('@')[0];
        let idx = phoneToIndex[jidPhone];
        if (idx === undefined && jidPhone.startsWith('55')) {
          idx = phoneToIndex[jidPhone.slice(2)];
        }

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
          state.replies[idx] = text.substring(0, 200);
          broadcast({ type: 'classification', index: idx, classification: 'quente', replyTime: state.replyTimes[idx], reply: state.replies[idx], hot: state.hot, warm: state.warm, cold: state.cold });
          saveDispatch(idx);
          return;
        }

        state.classifications[idx] = 'quente';
        state.replyTimes[idx] = now();
        state.replies[idx] = text.substring(0, 200);
        state.hot++;
        if (prev === 'morno') state.warm = Math.max(0, state.warm - 1);
        if (prev === 'frio')  state.cold = Math.max(0, state.cold - 1);

        broadcast({ type: 'classification', index: idx, classification: 'quente', replyTime: state.replyTimes[idx], reply: state.replies[idx], hot: state.hot, warm: state.warm, cold: state.cold });
        saveDispatch(idx);
        saveRunState();
        console.log(`🔥 Quente [${idx + 1}] ${contacts[idx].name} — "${text.substring(0, 60)}"`);
      });
    }
  } catch (e) {
    console.error('Webhook error:', e.message);
  }
});

// ── API routes ────────────────────────────────────────────────────────────────
app.get('/api/config', (req, res) => {
  res.json({ baseUrl: cfg.baseUrl, apiKey: cfg.apiKey ? '***' : '', instanceName: cfg.instanceName, webhookUrl: cfg.webhookUrl });
});

app.post('/api/config', async (req, res) => {
  const { baseUrl, apiKey, instanceName, webhookUrl } = req.body;
  if (baseUrl !== undefined)    cfg.baseUrl      = baseUrl.trim();
  if (apiKey && apiKey !== '***') cfg.apiKey     = apiKey.trim();
  if (instanceName)             cfg.instanceName = instanceName.trim();
  if (webhookUrl !== undefined) cfg.webhookUrl   = webhookUrl.trim();
  await saveConfig();
  res.json({ ok: true });
});

app.post('/api/instance/create', async (req, res) => {
  try {
    const d = await evoReq('post', '/instance/create', { instanceName: cfg.instanceName, qrcode: true, integration: 'WHATSAPP-BAILEYS' });
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
    await saveConfig();
    res.json({ ok: true, data: d, webhookUrl: webhookEndpoint });
  } catch (e) {
    res.json({ ok: false, error: e.response?.data?.message || e.message });
  }
});

app.get('/api/contacts', (req, res) => {
  res.json(contacts.map((c, i) => ({
    ...c,
    status:         state.statuses[i],
    timestamp:      state.timestamps[i],
    errorMsg:       state.errorMsgs[i],
    classification: state.classifications[i],
    readTime:       state.readTimes[i],
    replyTime:      state.replyTimes[i],
    reply:          state.replies[i]
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

app.post('/api/dispatch/settings', async (req, res) => {
  const { message, minDelay, maxDelay } = req.body;
  if (message   !== undefined) state.message   = message;
  if (minDelay  !== undefined) state.minDelay  = Math.max(0.5, parseFloat(minDelay));
  if (maxDelay  !== undefined) state.maxDelay  = Math.max(state.minDelay, parseFloat(maxDelay));
  await saveConfig();
  res.json({ ok: true });
});

app.post('/api/dispatch/start', async (req, res) => {
  if (state.running && !state.paused) return res.json({ ok: false, error: 'Já em execução' });
  const { message, minDelay, maxDelay } = req.body || {};
  if (message  !== undefined) state.message  = message;
  if (minDelay !== undefined) state.minDelay = Math.max(0.5, parseFloat(minDelay));
  if (maxDelay !== undefined) state.maxDelay = Math.max(state.minDelay, parseFloat(maxDelay));
  state.running = true;
  state.paused  = false;
  await saveConfig();
  await saveRunState();
  res.json({ ok: true });
  scheduleNext();
});

app.post('/api/dispatch/pause', async (req, res) => {
  state.paused = true;
  clearTimeout(state._dispatchTimer);
  clearInterval(state._countdownTimer);
  state.nextIn = 0;
  broadcast({ type: 'paused' });
  await saveRunState();
  res.json({ ok: true });
});

app.post('/api/dispatch/reset', async (req, res) => {
  clearTimeout(state._dispatchTimer);
  clearInterval(state._countdownTimer);
  state.running = false; state.paused = false;
  state.currentIndex = 0; state.sent = 0; state.errors = 0; state.skipped = 0;
  state.statuses         = new Array(TOTAL).fill('pending');
  state.timestamps       = new Array(TOTAL).fill('');
  state.errorMsgs        = new Array(TOTAL).fill('');
  state.classifications  = new Array(TOTAL).fill('none');
  state.readTimes        = new Array(TOTAL).fill('');
  state.replyTimes       = new Array(TOTAL).fill('');
  state.replies          = new Array(TOTAL).fill('');
  state.messageIds       = new Array(TOTAL).fill('');
  state.hot = 0; state.warm = 0; state.cold = 0; state.nextIn = 0;
  Object.keys(msgIdToIndex).forEach(k => delete msgIdToIndex[k]);
  broadcast({ type: 'reset' });
  await clearDispatchDB();
  res.json({ ok: true });
});

// ── Dispatch engine ───────────────────────────────────────────────────────────
async function sendOne(index) {
  const c = contacts[index];
  const phone = formatPhone(c.phone);

  if (!phone) {
    state.statuses[index]   = 'skipped';
    state.timestamps[index] = now();
    state.errorMsgs[index]  = 'Número inválido';
    state.skipped++;
    broadcast({ type: 'status', index, status: 'skipped', timestamp: state.timestamps[index], errorMsg: 'Número inválido' });
    await saveDispatch(index);
    return;
  }

  const text = state.message.replace(/\{nome\}/gi, c.name.split(' ')[0]);
  const resp = await evoReq('post', `/message/sendText/${cfg.instanceName}`, {
    number: phone, text, delay: 1200
  });

  const msgId = resp?.key?.id || resp?.id;
  if (msgId) {
    state.messageIds[index] = msgId;
    msgIdToIndex[msgId] = index;
  }
  phoneToIndex[phone] = index;
  if (phone.startsWith('55')) phoneToIndex[phone.slice(2)] = index;

  state.classifications[index] = 'frio';
  state.cold++;
}

function scheduleNext() {
  if (state.paused || !state.running) return;

  if (state.currentIndex >= TOTAL) {
    state.running = false;
    broadcast({ type: 'complete', sent: state.sent, errors: state.errors, skipped: state.skipped });
    saveRunState();
    console.log(`✅ Disparo concluído — Enviados: ${state.sent} | Erros: ${state.errors} | Inválidos: ${state.skipped}`);
    return;
  }

  const idx = state.currentIndex;
  state.statuses[idx] = 'sending';
  broadcast({ type: 'status', index: idx, status: 'sending' });

  sendOne(idx)
    .then(() => {
      if (state.statuses[idx] === 'skipped') return;
      state.statuses[idx]   = 'sent';
      state.timestamps[idx] = now();
      state.sent++;
      broadcast({ type: 'status', index: idx, status: 'sent', timestamp: state.timestamps[idx], classification: state.classifications[idx] });
      console.log(`✓ [${idx + 1}/${TOTAL}] ${contacts[idx].name}`);
    })
    .catch(e => {
      state.statuses[idx]   = 'error';
      state.timestamps[idx] = now();
      const errMsg = e.response?.data?.message || e.message || 'Erro';
      state.errorMsgs[idx] = errMsg;
      state.errors++;
      broadcast({ type: 'status', index: idx, status: 'error', timestamp: state.timestamps[idx], errorMsg: errMsg });
      console.error(`✗ [${idx + 1}/${TOTAL}] ${contacts[idx].name} — ${errMsg}`);
    })
    .finally(async () => {
      broadcast({ type: 'stats', sent: state.sent, errors: state.errors, skipped: state.skipped, hot: state.hot, warm: state.warm, cold: state.cold });
      state.currentIndex++;

      // Persist after each message
      await saveDispatch(idx);
      await saveRunState();

      if (state.paused || !state.running) return;
      if (state.currentIndex >= TOTAL) { scheduleNext(); return; }

      const minMs = state.minDelay * 60 * 1000;
      const maxMs = state.maxDelay * 60 * 1000;
      const delay = Math.round(minMs + Math.random() * (maxMs - minMs));
      const secs  = Math.round(delay / 1000);
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
    statuses:        state.statuses,
    timestamps:      state.timestamps,
    errorMsgs:       state.errorMsgs,
    classifications: state.classifications,
    readTimes:       state.readTimes,
    replyTimes:      state.replyTimes,
    replies:         state.replies,
    sent: state.sent, errors: state.errors, skipped: state.skipped,
    hot: state.hot, warm: state.warm, cold: state.cold,
    currentIndex: state.currentIndex,
    running: state.running, paused: state.paused, nextIn: state.nextIn,
    message: state.message, minDelay: state.minDelay, maxDelay: state.maxDelay
  }));
});

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;

(async () => {
  await loadFromDB();

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
})();

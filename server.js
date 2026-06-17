'use strict';
const express = require('express');
const http    = require('http');
const axios   = require('axios');
const path    = require('path');
const { createClient } = require('@supabase/supabase-js');

const app    = express();
const server = http.createServer(app);

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use('/assets', express.static(__dirname));

const contacts = require('./contacts');
const TOTAL    = contacts.length;

// ── Supabase ──────────────────────────────────────────────────────────────────
const supabase = createClient(
  process.env.SUPABASE_URL || 'https://gkbjemvwutaiksuueiqt.supabase.co',
  process.env.SUPABASE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdrYmplbXZ3dXRhaWtzdXVlaXF0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODAxMDg2OTQsImV4cCI6MjA5NTY4NDY5NH0.wZS9W4aLr68KEpkxHhHTakjibzRNxGAqgckz3n9VovQ'
);

// ── Config ────────────────────────────────────────────────────────────────────
let cfg = { baseUrl: '', apiKey: '', instanceName: 'fastescova', webhookUrl: '' };

// ── Dispatch state (in-memory) ────────────────────────────────────────────────
let state = {
  running: false, paused: false,
  currentIndex: 0, sent: 0, errors: 0, skipped: 0,
  statuses:        new Array(TOTAL).fill('pending'),
  timestamps:      new Array(TOTAL).fill(''),
  errorMsgs:       new Array(TOTAL).fill(''),
  classifications: new Array(TOTAL).fill('none'),
  readTimes:       new Array(TOTAL).fill(''),
  replyTimes:      new Array(TOTAL).fill(''),
  replies:         new Array(TOTAL).fill(''),
  messageIds:      new Array(TOTAL).fill(''),
  hot: 0, warm: 0, cold: 0,
  message:  'Olá {nome}! Temos novidades incríveis na Fast Escova esperando por você! 💛✨',
  minDelay: 1, maxDelay: 3,
  _dispatchTimer: null, _countdownTimer: null
};

// ── Phone lookup maps ─────────────────────────────────────────────────────────
const phoneToIndex = {};
contacts.forEach((c, i) => {
  const d = c.phone.replace(/\D/g, '');
  if (d.length < 8) return;
  phoneToIndex[d] = i;
  if (d.startsWith('55') && d.length >= 12) phoneToIndex[d.slice(2)] = i;
  else if (!d.startsWith('55') && d.length >= 10) phoneToIndex['55' + d] = i;
});
const msgIdToIndex = {};

// ── Supabase persistence ──────────────────────────────────────────────────────
async function loadFromDB() {
  try {
    const { data: c } = await supabase.from('fast_config').select('*').eq('id', 1).single();
    if (c) {
      cfg.baseUrl      = c.base_url      || '';
      cfg.apiKey       = c.api_key       || '';
      cfg.instanceName = c.instance_name || 'fastescova';
      cfg.webhookUrl   = c.webhook_url   || '';
      if (c.message)    state.message   = c.message;
      if (c.min_delay)  state.minDelay  = parseFloat(c.min_delay);
      if (c.max_delay)  state.maxDelay  = parseFloat(c.max_delay);
    }
    const { data: run } = await supabase.from('fast_run_state').select('*').eq('id', 1).single();
    if (run) {
      state.currentIndex = run.current_index || 0;
      state.sent    = run.sent    || 0;
      state.errors  = run.errors  || 0;
      state.skipped = run.skipped || 0;
      state.hot     = run.hot     || 0;
      state.warm    = run.warm    || 0;
      state.cold    = run.cold    || 0;
    }
    const { data: rows } = await supabase.from('fast_dispatch').select('*');
    if (rows) rows.forEach(r => {
      state.statuses[r.idx]        = r.status        || 'pending';
      state.timestamps[r.idx]      = r.sent_at       || '';
      state.classifications[r.idx] = r.classification || 'none';
      state.readTimes[r.idx]       = r.read_time     || '';
      state.replies[r.idx]         = r.reply         || '';
      state.replyTimes[r.idx]      = r.reply_time    || '';
      state.messageIds[r.idx]      = r.message_id    || '';
      if (r.message_id) msgIdToIndex[r.message_id] = r.idx;
    });
    console.log('  ✅  Estado restaurado do Supabase');
  } catch (e) {
    console.warn('  ⚠️  Supabase load falhou:', e.message);
  }
}

async function saveConfig() {
  await supabase.from('fast_config').upsert({
    id: 1,
    base_url: cfg.baseUrl, api_key: cfg.apiKey,
    instance_name: cfg.instanceName, webhook_url: cfg.webhookUrl,
    message: state.message, min_delay: state.minDelay, max_delay: state.maxDelay,
    updated_at: new Date().toISOString()
  }).catch(e => console.warn('saveConfig:', e.message));
}

async function saveRunState(extra = {}) {
  await supabase.from('fast_run_state').upsert({
    id: 1,
    running: state.running, paused: state.paused,
    current_index: state.currentIndex,
    sent: state.sent, errors: state.errors, skipped: state.skipped,
    hot: state.hot, warm: state.warm, cold: state.cold,
    updated_at: new Date().toISOString(),
    ...extra
  }).catch(e => console.warn('saveRunState:', e.message));
}

async function saveDispatch(i) {
  await supabase.from('fast_dispatch').upsert({
    idx: i,
    status:         state.statuses[i]        || 'pending',
    sent_at:        state.timestamps[i]      || '',
    classification: state.classifications[i] || 'none',
    read_time:      state.readTimes[i]       || '',
    reply:          state.replies[i]         || '',
    reply_time:     state.replyTimes[i]      || '',
    message_id:     state.messageIds[i]      || '',
    updated_at:     new Date().toISOString()
  }).catch(e => console.warn('saveDispatch:', e.message));
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function formatPhone(raw) {
  const d = raw.replace(/\D/g, '');
  if (d.length < 8 || /^0+$/.test(d) || /^1+$/.test(d) || d.startsWith('000')) return null;
  return d.startsWith('55') && d.length >= 12 ? d : '55' + d;
}

async function evoReq(method, endpoint, data) {
  const url = cfg.baseUrl.replace(/\/$/, '') + '/' + endpoint.replace(/^\//, '');
  const res = await axios({ method, url, data, headers: { apikey: cfg.apiKey, 'Content-Type': 'application/json' }, timeout: 20000 });
  return res.data;
}

function now() {
  return new Date().toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

// ── Webhook ───────────────────────────────────────────────────────────────────
app.post('/webhook', (req, res) => {
  res.sendStatus(200);
  try {
    const event = (req.body.event || req.body.type || '').toLowerCase().replace(/_/g, '.').replace(/\s/g, '');
    const data  = req.body.data;

    if (event === 'messages.update') {
      const updates = Array.isArray(data) ? data : (data ? [data] : []);
      updates.forEach(upd => {
        const idx    = msgIdToIndex[upd?.key?.id];
        const status = upd?.update?.status ?? upd?.status;
        if (idx === undefined) return;
        const isRead = status === 4 || status === 'READ';
        if (!isRead || state.classifications[idx] === 'quente') return;
        const prev = state.classifications[idx];
        if (prev === 'morno') return;
        state.classifications[idx] = 'morno';
        state.readTimes[idx] = now();
        state.warm++;
        if (prev === 'frio') state.cold = Math.max(0, state.cold - 1);
        saveDispatch(idx);
        saveRunState();
        console.log(`🌡️  Morno [${idx + 1}] ${contacts[idx].name}`);
      });
    }

    if (event === 'messages.upsert') {
      const msgs = Array.isArray(data?.messages) ? data.messages : Array.isArray(data) ? data : (data ? [data] : []);
      msgs.forEach(msg => {
        if (msg?.key?.fromMe) return;
        const jid = (msg?.key?.remoteJid || '').split('@')[0];
        if ((msg?.key?.remoteJid || '').endsWith('@g.us')) return;
        let idx = phoneToIndex[jid];
        if (idx === undefined && jid.startsWith('55')) idx = phoneToIndex[jid.slice(2)];
        const quoted = msg?.message?.extendedTextMessage?.contextInfo?.stanzaId || msg?.contextInfo?.stanzaId;
        if (idx === undefined && quoted) idx = msgIdToIndex[quoted];
        if (idx === undefined) return;

        const text = msg?.message?.conversation || msg?.message?.extendedTextMessage?.text || msg?.message?.imageMessage?.caption || '[mídia]';
        const prev = state.classifications[idx];
        state.classifications[idx] = 'quente';
        state.replies[idx]         = text.substring(0, 200);
        state.replyTimes[idx]      = now();
        if (prev !== 'quente') {
          state.hot++;
          if (prev === 'morno') state.warm = Math.max(0, state.warm - 1);
          if (prev === 'frio')  state.cold = Math.max(0, state.cold - 1);
        }
        saveDispatch(idx);
        saveRunState();
        console.log(`🔥 Quente [${idx + 1}] ${contacts[idx].name}`);
      });
    }
  } catch (e) {
    console.error('Webhook error:', e.message);
  }
});

// ── API routes ────────────────────────────────────────────────────────────────
app.get('/api/config', (req, res) => res.json({ baseUrl: cfg.baseUrl, apiKey: cfg.apiKey ? '***' : '', instanceName: cfg.instanceName, webhookUrl: cfg.webhookUrl }));

app.post('/api/config', async (req, res) => {
  const { baseUrl, apiKey, instanceName, webhookUrl } = req.body;
  if (baseUrl !== undefined)        cfg.baseUrl      = baseUrl.trim();
  if (apiKey && apiKey !== '***')   cfg.apiKey       = apiKey.trim();
  if (instanceName)                 cfg.instanceName = instanceName.trim();
  if (webhookUrl !== undefined)     cfg.webhookUrl   = webhookUrl.trim();
  await saveConfig();
  res.json({ ok: true });
});

app.post('/api/instance/create', async (req, res) => {
  try { res.json({ ok: true, data: await evoReq('post', '/instance/create', { instanceName: cfg.instanceName, qrcode: true, integration: 'WHATSAPP-BAILEYS' }) }); }
  catch (e) { res.json({ ok: false, error: e.response?.data?.message || e.message }); }
});

app.get('/api/instance/qr', async (req, res) => {
  try { res.json({ ok: true, data: await evoReq('get', `/instance/connect/${cfg.instanceName}`) }); }
  catch (e) { res.json({ ok: false, error: e.response?.data?.message || e.message }); }
});

app.post('/api/instance/pairing-code', async (req, res) => {
  try { res.json({ ok: true, data: await evoReq('post', `/instance/pairingCode/${cfg.instanceName}`, { phoneNumber: req.body.phoneNumber }) }); }
  catch (e) { res.json({ ok: false, error: e.response?.data?.message || e.message }); }
});

app.get('/api/instance/status', async (req, res) => {
  try { res.json({ ok: true, data: await evoReq('get', `/instance/connectionState/${cfg.instanceName}`) }); }
  catch (e) { res.json({ ok: false, error: e.response?.data?.message || e.message }); }
});

app.post('/api/webhook/configure', async (req, res) => {
  try {
    if (req.body.webhookUrl) cfg.webhookUrl = req.body.webhookUrl.trim();
    if (!cfg.webhookUrl) return res.json({ ok: false, error: 'Informe a URL pública do servidor' });
    const endpoint = cfg.webhookUrl.replace(/\/$/, '') + '/webhook';
    const d = await evoReq('post', `/webhook/set/${cfg.instanceName}`, { url: endpoint, events: ['MESSAGES_UPDATE', 'MESSAGES_UPSERT', 'CONNECTION_UPDATE'], webhook_by_events: false, webhook_base64: false });
    await saveConfig();
    res.json({ ok: true, data: d, webhookUrl: endpoint });
  } catch (e) { res.json({ ok: false, error: e.response?.data?.message || e.message }); }
});

app.get('/api/contacts', (req, res) => {
  res.json(contacts.map((c, i) => ({
    ...c,
    status: state.statuses[i], timestamp: state.timestamps[i], errorMsg: state.errorMsgs[i],
    classification: state.classifications[i], readTime: state.readTimes[i],
    replyTime: state.replyTimes[i], reply: state.replies[i]
  })));
});

app.post('/api/dispatch/settings', async (req, res) => {
  const { message, minDelay, maxDelay } = req.body;
  if (message  !== undefined) state.message  = message;
  if (minDelay !== undefined) state.minDelay = Math.max(0.5, parseFloat(minDelay));
  if (maxDelay !== undefined) state.maxDelay = Math.max(state.minDelay, parseFloat(maxDelay));
  await saveConfig();
  res.json({ ok: true });
});

app.post('/api/dispatch/start', async (req, res) => {
  if (state.running && !state.paused) return res.json({ ok: false, error: 'Já em execução' });
  const { message, minDelay, maxDelay } = req.body || {};
  if (message  !== undefined) state.message  = message;
  if (minDelay !== undefined) state.minDelay = Math.max(0.5, parseFloat(minDelay));
  if (maxDelay !== undefined) state.maxDelay = Math.max(state.minDelay, parseFloat(maxDelay));
  state.running = true; state.paused = false;
  await saveConfig();
  await saveRunState();
  res.json({ ok: true });
  scheduleNext();
});

app.post('/api/dispatch/pause', async (req, res) => {
  state.paused = true;
  clearTimeout(state._dispatchTimer);
  clearInterval(state._countdownTimer);
  await saveRunState({ next_message_at: null });
  res.json({ ok: true });
});

app.post('/api/dispatch/reset', async (req, res) => {
  clearTimeout(state._dispatchTimer);
  clearInterval(state._countdownTimer);
  state.running = false; state.paused = false;
  state.currentIndex = 0; state.sent = 0; state.errors = 0; state.skipped = 0;
  state.statuses        = new Array(TOTAL).fill('pending');
  state.timestamps      = new Array(TOTAL).fill('');
  state.errorMsgs       = new Array(TOTAL).fill('');
  state.classifications = new Array(TOTAL).fill('none');
  state.readTimes       = new Array(TOTAL).fill('');
  state.replyTimes      = new Array(TOTAL).fill('');
  state.replies         = new Array(TOTAL).fill('');
  state.messageIds      = new Array(TOTAL).fill('');
  state.hot = 0; state.warm = 0; state.cold = 0;
  Object.keys(msgIdToIndex).forEach(k => delete msgIdToIndex[k]);
  // Clear DB
  await supabase.from('fast_dispatch').delete().neq('idx', -1).catch(() => {});
  await saveRunState({ next_message_at: null, reset_at: new Date().toISOString() });
  res.json({ ok: true });
});

// ── Dispatch engine ───────────────────────────────────────────────────────────
async function sendOne(i) {
  const c     = contacts[i];
  const phone = formatPhone(c.phone);
  if (!phone) {
    state.statuses[i]   = 'skipped';
    state.timestamps[i] = now();
    state.errorMsgs[i]  = 'Número inválido';
    state.skipped++;
    await saveDispatch(i);
    return;
  }
  const text = state.message.replace(/\{nome\}/gi, c.name.split(' ')[0]);
  const resp = await evoReq('post', `/message/sendText/${cfg.instanceName}`, { number: phone, text, delay: 1200 });
  const msgId = resp?.key?.id || resp?.id;
  if (msgId) { state.messageIds[i] = msgId; msgIdToIndex[msgId] = i; }
  phoneToIndex[phone] = i;
  if (phone.startsWith('55')) phoneToIndex[phone.slice(2)] = i;
  state.classifications[i] = 'frio';
  state.cold++;
}

function scheduleNext() {
  if (state.paused || !state.running) return;
  if (state.currentIndex >= TOTAL) {
    state.running = false;
    saveRunState({ next_message_at: null });
    console.log(`✅ Concluído — ${state.sent} enviados | ${state.errors} erros | ${state.skipped} inválidos`);
    return;
  }

  const idx = state.currentIndex;
  state.statuses[idx] = 'sending';

  sendOne(idx)
    .then(() => {
      if (state.statuses[idx] === 'skipped') return;
      state.statuses[idx]   = 'sent';
      state.timestamps[idx] = now();
      state.sent++;
      console.log(`✓ [${idx + 1}/${TOTAL}] ${contacts[idx].name}`);
    })
    .catch(e => {
      state.statuses[idx]   = 'error';
      state.timestamps[idx] = now();
      state.errorMsgs[idx]  = e.response?.data?.message || e.message || 'Erro';
      state.errors++;
      console.error(`✗ [${idx + 1}/${TOTAL}] ${contacts[idx].name} — ${state.errorMsgs[idx]}`);
    })
    .finally(async () => {
      state.currentIndex++;
      await saveDispatch(idx);
      await saveRunState();

      if (state.paused || !state.running) return;
      if (state.currentIndex >= TOTAL) { scheduleNext(); return; }

      const minMs = state.minDelay * 60 * 1000;
      const maxMs = state.maxDelay * 60 * 1000;
      const delay = Math.round(minMs + Math.random() * (maxMs - minMs));
      const nextAt = new Date(Date.now() + delay).toISOString();

      await saveRunState({ next_message_at: nextAt });

      state._dispatchTimer = setTimeout(() => scheduleNext(), delay);
    });
}

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

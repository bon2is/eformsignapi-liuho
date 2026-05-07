require('dotenv').config();
const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');
const path = require('path');
const crypto = require('crypto');

const app = express();
const DEFAULT_PORT = parseInt(process.env.PORT, 10) || 3001;
let PORT = DEFAULT_PORT;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ============================================================
// IN-MEMORY STORE
// ============================================================
let webhookEvents = [];       // 웹훅 수신 이력
let sseClients = [];          // SSE 연결된 브라우저 목록

// ============================================================
// SSE: 실시간 이벤트 스트림 (브라우저 연결용)
// ============================================================
app.get('/api/events', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.flushHeaders();

  const clientId = Date.now();
  const client = { id: clientId, res };
  sseClients.push(client);

  // 연결 즉시 현재 이벤트 이력 전송
  res.write(`data: ${JSON.stringify({ type: 'init', events: webhookEvents })}\n\n`);

  // 연결 유지 ping (30초마다)
  const ping = setInterval(() => {
    res.write(`: ping\n\n`);
  }, 30000);

  req.on('close', () => {
    clearInterval(ping);
    sseClients = sseClients.filter(c => c.id !== clientId);
  });
});

function broadcastSSE(data) {
  const payload = `data: ${JSON.stringify(data)}\n\n`;
  sseClients.forEach(client => {
    try { client.res.write(payload); } catch (e) { /* 연결 끊긴 클라이언트 무시 */ }
  });
}

// ============================================================
// WEBHOOK: eformsign → 이 서버 수신
// ============================================================
app.post('/api/webhook', async (req, res) => {
  const event = req.body;
  const receivedAt = new Date().toISOString();

  console.log('\n[WEBHOOK] 수신:', JSON.stringify(event, null, 2));

  const logEntry = {
    id: crypto.randomUUID(),
    received_at: receivedAt,
    event_type: event.doc_status || event.event_type || 'unknown',
    document_id: event.document_id || event.doc_id || '-',
    document_name: event.document_name || event.doc_name || '-',
    signer_name: event.participants?.[0]?.name || event.signer_name || '-',
    signer_email: event.participants?.[0]?.email || '-',
    completed_at: event.updated_date || event.completed_at || receivedAt,
    raw: event,
    archive_status: 'pending',
    archive_ref: null,
    download_url: null,
  };

  webhookEvents.unshift(logEntry);

  // SSE로 즉시 브로드캐스트
  broadcastSSE({ type: 'webhook_received', event: logEntry });

  // Mock 핵심 시스템 귀속 (1.5초 후 시뮬레이션)
  setTimeout(() => {
    logEntry.archive_status = 'success';
    logEntry.archive_ref = `CORE-LH-${Date.now()}`;
    logEntry.download_url = `${process.env.EFORMSIGN_SERVER_URL}/api/documents/${logEntry.document_id}/download`;

    console.log(`[ARCHIVE] 핵심 시스템 귀속 완료: ${logEntry.archive_ref}`);
    broadcastSSE({ type: 'archive_complete', event: logEntry });
  }, 1500);

  res.status(200).json({ result: 'SUCCESS', received_at: receivedAt });
});

// ============================================================
// WEBHOOK LOG: 전체 이력 조회
// ============================================================
app.get('/api/webhook/log', (req, res) => {
  res.json({ count: webhookEvents.length, events: webhookEvents });
});

// ============================================================
// REMOTE SIGN: eformsign API 프록시 (API key 서버 측 보관)
// ============================================================
app.post('/api/remote-sign', async (req, res) => {
  const { policy, customer, vehicle, documentType } = req.body;

  const companyId = process.env.EFORMSIGN_COMPANY_ID;
  const templateId = process.env.EFORMSIGN_TEMPLATE_ID;
  const apiKey = process.env.EFORMSIGN_API_KEY;
  const serverUrl = process.env.EFORMSIGN_SERVER_URL;

  if (!companyId || !templateId || !apiKey) {
    return res.status(500).json({ error: 'eformsign 환경변수 미설정' });
  }

  const today = new Date().toISOString().split('T')[0];
  const docTypeLabel = documentType === 'new' ? '新保要保書' : '續保要保書';

  const documentOption = {
    company: { id: companyId, country_code: 'tw' },
    mode: { type: '01', template_id: templateId },
    user: {
      type: '02',
      external_user_info: {
        name: customer.name,
      },
    },
    layout: { lang_code: 'zh-TW' },
    prefill_data: {
      document_name: `${docTypeLabel}_${customer.name}_${policy.policy_id}_${today}`,
      fields: buildPrefillFields(policy, customer, vehicle, documentType, today),
    },
  };

  try {
    // eformsign API: 원격 서명 URL 생성
    const apiRes = await fetch(`${serverUrl}/api/v2.0/documents/remote`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'eformsign_signature': apiKey,
      },
      body: JSON.stringify(documentOption),
    });

    const data = await apiRes.json();

    if (data.code === '00') {
      console.log('[REMOTE-SIGN] URL 생성 성공:', data.signing_url);
      return res.json({
        success: true,
        signing_url: data.signing_url,
        document_id: data.document_id,
      });
    } else {
      console.warn('[REMOTE-SIGN] API 응답 오류:', data);
      // Demo fallback: 실제 API 연동 전 Mock URL 반환
      return res.json({
        success: true,
        mock: true,
        signing_url: `${serverUrl}/sign/demo-${Date.now()}`,
        document_id: `DEMO-${Date.now()}`,
        message: 'Demo 모드: 실제 eformsign 템플릿 연동 후 실 URL 반환',
      });
    }
  } catch (err) {
    console.error('[REMOTE-SIGN] 네트워크 오류:', err.message);
    // Demo fallback
    return res.json({
      success: true,
      mock: true,
      signing_url: `https://sg.eformsign.com/sign/demo-${Date.now()}`,
      document_id: `DEMO-${Date.now()}`,
      message: 'Demo 모드 (네트워크 오류 fallback)',
    });
  }
});

// ============================================================
// PREFILL FIELDS BUILDER
// ============================================================
function buildPrefillFields(policy, customer, vehicle, documentType, today) {
  const docTypeLabel = documentType === 'new' ? '新保' : '續保';
  return [
    { id: 'policy_number',   value: policy.policy_id,       enabled: false },
    { id: 'policy_type',     value: docTypeLabel,            enabled: false },
    { id: 'customer_name',   value: customer.name,           enabled: false },
    { id: 'customer_name_en',value: customer.name_en,        enabled: false },
    { id: 'customer_id',     value: customer.id_number,      enabled: false },
    { id: 'customer_birth',  value: customer.birth_date,     enabled: false },
    { id: 'customer_phone',  value: customer.phone,          enabled: true  },
    { id: 'customer_email',  value: customer.email,          enabled: true  },
    { id: 'vehicle_plate',   value: vehicle.plate,           enabled: false },
    { id: 'vehicle_make',    value: vehicle.make,            enabled: false },
    { id: 'vehicle_model',   value: vehicle.model,           enabled: false },
    { id: 'vehicle_year',    value: String(vehicle.year),    enabled: false },
    { id: 'insurer',         value: policy.insurer,          enabled: false },
    { id: 'premium',         value: String(policy.premium),  enabled: false },
    { id: 'coverage_start',  value: policy.coverage_start,   enabled: false },
    { id: 'coverage_end',    value: policy.coverage_end,     enabled: false },
    { id: 'sign_date',       value: today,                   enabled: false },
  ];
}

// ============================================================
// WEBHOOK TEST: curl용 테스트 엔드포인트
// ============================================================
app.post('/api/webhook/test', (req, res) => {
  const mockPayload = {
    doc_status: 'document_completed',
    document_id: `TEST-${Date.now()}`,
    document_name: `車險要保書_테스트_${new Date().toLocaleDateString('zh-TW')}`,
    signer_name: '陳大華',
    updated_date: new Date().toISOString(),
    participants: [
      { name: '陳大華', email: 'chen@gmail.com', signed_at: new Date().toISOString() }
    ],
  };

  // 자신의 웹훅으로 POST
  fetch(`http://localhost:${PORT}/api/webhook`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(mockPayload),
  }).catch(console.error);

  res.json({ message: '테스트 웹훅 발송 완료', payload: mockPayload });
});

// ============================================================
// START
// ============================================================
function startServer(port) {
  app.listen(port, () => {
    console.log(`\n🚗 Liu Ho Insurance Demo Server`);
    console.log(`   http://localhost:${port}`);
    console.log(`\n웹훅 수신 엔드포인트: POST http://localhost:${port}/api/webhook`);
    console.log(`테스트: POST http://localhost:${port}/api/webhook/test\n`);
  }).on('error', err => {
    if (err.code === 'EADDRINUSE') {
      console.warn(`Port ${port} is already in use. Trying port ${port + 1}...`);
      PORT = port + 1;
      startServer(PORT);
    } else {
      console.error('Server failed to start:', err);
      process.exit(1);
    }
  });
}

startServer(PORT);

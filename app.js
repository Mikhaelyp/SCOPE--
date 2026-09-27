const axios = require('axios');
const cron = require('node-cron');
const fs = require('fs');
const path = require('path');
const https = require('https');
const express = require('express');
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const qrcodeTerminal = require('qrcode-terminal');

// Global error handlers to prevent crash on Baileys pre-key sync timeout
process.on('uncaughtException', (err) => {
  console.error('[UNCAUGHT EXCEPTION]', err.message);
});
process.on('unhandledRejection', (reason) => {
  console.error('[UNHANDLED REJECTION]', reason?.message || reason);
});

// File Paths
const CONFIG_FILE = path.join(__dirname, 'config.json');
const LOG_FILE = path.join(__dirname, 'logger.json');
const ERRORS_FILE = path.join(__dirname, 'errors.json');
const AUTH_DIR = path.join(__dirname, 'auth_info_baileys');
const PUBLIC_DIR = path.join(__dirname, 'public');

// Default Configuration
const DEFAULT_CONFIG = {
  CRON_SCHEDULE: '*/10 * * * * *', // Background check interval
  FAST_POLL_INTERVAL_MS: 1500,     // Polling active jobs interval (1.5s ultra-fast parallel)
  TARGET_SERVERS: [
    {
      id: 'srv-jobs-3013',
      name: 'JOBS 3013',
      baseUrl: 'https://jobs.asiatop.co.id:3013',
      apiUrl: 'https://jobs.asiatop.co.id:3013/api/app/get_schedule',
      apiKey: 'e495e84af0472ba2f71ebe19ffbdb005',
      type: 'cronacle',
      enabled: true
    },
    {
      id: 'srv-jobs-3012',
      name: 'JOBS 3012',
      baseUrl: 'https://sfa.asiatop.co.id:3012',
      apiUrl: 'https://sfa.asiatop.co.id:3012/api/app/get_schedule',
      apiKey: '',
      type: 'cronacle',
      enabled: true
    }
  ],
  WA_TARGET_JID: '6285776870226@s.whatsapp.net',
  WA_TARGET_ROUTING: [
    {
      id: 'target-primary',
      type: 'private',
      name: 'Admin Utama (Pribadi)',
      target: '6285776870226@s.whatsapp.net',
      enabled: true
    }
  ],
  IGNORE_SSL_ERRORS: true,
  PORT: 3000,
  AUTO_ALERT_RETRY: true,
  ALERT_DELAY_MINUTES: 0,        // 0 = Instan seketika, >0 = Tunda alert per menit
  SUMMARY_ENABLED: true,         // Toggle rekap summary berkala
  SUMMARY_INTERVAL_MINUTES: 30,  // Interval rekap summary (menit)
  SUMMARY_ONLY_IF_ERROR: true,   // Hanya kirim jika ada error aktif
  ALERT_TEMPLATE: "🚨 *ALERT: JOB SCHEDULER FAILURE* 🚨\n\n📌 *Server:* {serverName}\n❌ *Job Name:* {jobName}\n📊 *Status:* {status}\n⏰ *Execution Time:* {lastRunTime}\n🔗 *Dashboard:* {dashboardUrl}\n\n*Action Required:* Mohon segera lakukan verifikasi/re-run job.",
  SUMMARY_TEMPLATE: "📊 *REKAP SUMMARY JOB GAGAL (PERIODIC DIGEST)* 📊\n\n⏰ *Waktu Pemantauan:* {timestamp}\n⚠️ *Total Job Error Aktif:* {totalErrors} Insiden\n\n{errorList}\n\n🔗 *Dashboard Monitoring:* {dashboardUrl}\n_Laporan rekap berkala otomatis setiap {interval} menit._"
};

function loadConfig() {
  if (!fs.existsSync(CONFIG_FILE)) {
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(DEFAULT_CONFIG, null, 2));
    return DEFAULT_CONFIG;
  }
  try {
    const raw = fs.readFileSync(CONFIG_FILE, 'utf-8');
    return { ...DEFAULT_CONFIG, ...JSON.parse(raw) };
  } catch (err) {
    console.error('Gagal membaca config.json:', err.message);
    return DEFAULT_CONFIG;
  }
}

function saveConfig(newConfig) {
  currentConfig = { ...currentConfig, ...newConfig };
  fs.writeFileSync(CONFIG_FILE, JSON.stringify(currentConfig, null, 2));
  return currentConfig;
}

let currentConfig = loadConfig();

// App Express
const app = express();
app.use(express.json());
app.use(express.static(PUBLIC_DIR));

let waSock = null;
let isConnected = false;
let waUserInfo = null;
let currentQrCode = null;
let lastCheckTime = null;
let isChecking = false;
let isFastPolling = false;
let cronTask = null;
let fastPollTimer = null;

// SSE Client Connections
const sseClients = new Set();

// In-Memory Message Retry & Decryption Cache (Solves "Waiting for this message" E2EE delay)
class MemoryCacheStore {
  constructor() {
    this.cache = new Map();
  }
  get(key) {
    return this.cache.get(key);
  }
  set(key, value) {
    this.cache.set(key, value);
  }
  del(key) {
    this.cache.delete(key);
  }
  flushAll() {
    this.cache.clear();
  }
}

const msgRetryCounterCache = new MemoryCacheStore();
const sentMessagesStore = new Map();

function storeSentMessage(sentMsg) {
  if (sentMsg?.key?.id && sentMsg.message) {
    sentMessagesStore.set(sentMsg.key.id, sentMsg.message);
    if (sentMessagesStore.size > 1000) {
      const firstKey = sentMessagesStore.keys().next().value;
      sentMessagesStore.delete(firstKey);
    }
  }
}

let monitoringStats = {
  totalChecks: 0,
  totalAlertsSent: 0,
  recentAlerts: [],
  detectedErrors: [],
  activeErrors: [],
  recoveredErrors: [],
  serverStatus: {},
  jobsCache: {},
  activeJobsCache: {},
  systemLogs: []
};

function logSystem(type, message, details = null) {
  const logItem = {
    id: Date.now() + Math.random().toString(36).substr(2, 4),
    time: new Date().toLocaleTimeString('id-ID'),
    type,
    message,
    details
  };
  monitoringStats.systemLogs.unshift(logItem);
  if (monitoringStats.systemLogs.length > 100) monitoringStats.systemLogs.pop();
  console.log(`[${type.toUpperCase()}] ${message}`);
  broadcastStateToClients();
}

// Storage Helpers
function ensureLogFile() {
  if (!fs.existsSync(LOG_FILE)) {
    fs.writeFileSync(LOG_FILE, JSON.stringify([], null, 2));
  }
}

function getSentAlerts() {
  ensureLogFile();
  try {
    const rawData = fs.readFileSync(LOG_FILE, 'utf-8');
    return JSON.parse(rawData);
  } catch (error) {
    return [];
  }
}

function saveSentAlert(alertKey, details = {}) {
  try {
    const alerts = getSentAlerts();
    alerts.push({ key: alertKey, date: new Date().toISOString(), ...details });
    if (alerts.length > 500) alerts.splice(0, alerts.length - 500);
    fs.writeFileSync(LOG_FILE, JSON.stringify(alerts, null, 2));
  } catch (error) {
    console.error('Error menyimpan ke logger.json:', error.message);
  }
}

function clearSentAlerts() {
  fs.writeFileSync(LOG_FILE, JSON.stringify([], null, 2));
  monitoringStats.recentAlerts = [];
  broadcastStateToClients();
  return true;
}

function loadPersistentErrors() {
  if (!fs.existsSync(ERRORS_FILE)) {
    fs.writeFileSync(ERRORS_FILE, JSON.stringify([], null, 2));
    return [];
  }
  try {
    const raw = fs.readFileSync(ERRORS_FILE, 'utf-8');
    return JSON.parse(raw);
  } catch (e) {
    return [];
  }
}

function savePersistentErrors(errorsList) {
  try {
    if (errorsList.length > 1000) errorsList = errorsList.slice(0, 1000);
    fs.writeFileSync(ERRORS_FILE, JSON.stringify(errorsList, null, 2));
  } catch (e) {
    console.error('Error menyimpan errors.json:', e.message);
  }
}

function clearPersistentErrors() {
  fs.writeFileSync(ERRORS_FILE, JSON.stringify([], null, 2));
  monitoringStats.detectedErrors = [];
  monitoringStats.activeErrors = [];
  monitoringStats.recoveredErrors = [];
  broadcastStateToClients();
  return true;
}

// Build Full State Payload with CPU & Memory Metrics
function getSystemStatePayload() {
  let allJobs = [];
  let allActiveJobs = [];

  Object.keys(monitoringStats.jobsCache).forEach(serverName => {
    allJobs = allJobs.concat(monitoringStats.jobsCache[serverName]);
  });

  Object.keys(monitoringStats.activeJobsCache).forEach(serverName => {
    allActiveJobs = allActiveJobs.concat(monitoringStats.activeJobsCache[serverName]);
  });

  // Calculate live aggregate CPU & Memory usage across active processes
  let totalCpuSum = 0;
  let cpuCount = 0;
  let totalMemBytesSum = 0;
  let memCount = 0;

  allActiveJobs.forEach(job => {
    if (job.cpuRaw && typeof job.cpuRaw === 'number') {
      totalCpuSum += job.cpuRaw;
      cpuCount++;
    }
    if (job.memRaw && typeof job.memRaw === 'number') {
      totalMemBytesSum += job.memRaw;
      memCount++;
    }
  });

  // Server master memory from status
  let masterRssBytes = 0;
  Object.values(monitoringStats.serverStatus).forEach(s => {
    if (s.rawMem?.rss) masterRssBytes += s.rawMem.rss;
  });

  const avgCpuVal = cpuCount > 0 ? (totalCpuSum / cpuCount).toFixed(1) : '0.0';
  const totalCpuVal = totalCpuSum.toFixed(1);
  const totalActiveMemMB = (totalMemBytesSum / (1024 * 1024)).toFixed(1);
  const totalSystemMemMB = ((totalMemBytesSum + masterRssBytes) / (1024 * 1024)).toFixed(1);
  const avgMemPerJobMB = memCount > 0 ? (totalMemBytesSum / memCount / (1024 * 1024)).toFixed(1) : '0.0';

  return {
    status: 'online',
    timestamp: Date.now(),
    whatsapp: {
      connected: isConnected,
      user: waUserInfo,
      hasPendingQr: !isConnected && !!currentQrCode,
      qrCode: currentQrCode
    },
    config: currentConfig,
    lastCheckTime,
    isChecking,
    stats: {
      totalChecks: monitoringStats.totalChecks,
      totalAlertsSent: monitoringStats.totalAlertsSent,
      totalJobsMonitored: allJobs.length,
      totalErrorsCurrent: monitoringStats.detectedErrors.length,
      activeErrorsCount: monitoringStats.activeErrors.length,
      recoveredErrorsCount: monitoringStats.recoveredErrors.length,
      totalRunningCurrent: allActiveJobs.length,
      
      // Resource Performance Metrics
      avgCpuUsage: `${avgCpuVal}%`,
      totalCpuUsage: `${totalCpuVal}%`,
      totalMemUsage: `${totalActiveMemMB} MB`,
      systemMemUsage: `${totalSystemMemMB} MB`,
      avgMemUsage: `${avgMemPerJobMB} MB`,
      
      serverStatus: monitoringStats.serverStatus,
      recentAlerts: monitoringStats.recentAlerts,
      detectedErrors: monitoringStats.detectedErrors,
      systemLogs: monitoringStats.systemLogs.slice(0, 30)
    },
    activeJobs: allActiveJobs,
    jobs: allJobs
  };
}

// Broadcast Real-time State via SSE
function broadcastStateToClients() {
  if (sseClients.size === 0) return;
  const payloadStr = JSON.stringify(getSystemStatePayload());
  const message = `event: state\ndata: ${payloadStr}\n\n`;

  for (const client of sseClients) {
    try {
      client.res.write(message);
    } catch (e) {
      sseClients.delete(client);
    }
  }
}

function formatJid(inputJid) {
  if (!inputJid) return '';
  let str = String(inputJid).trim();
  if (str.includes('@')) return str;
  str = str.replace(/[^0-9]/g, '');
  if (str.startsWith('0')) {
    str = '62' + str.substring(1);
  }
  return `${str}@s.whatsapp.net`;
}

function getActiveTargetJids(customTarget = null) {
  if (customTarget) {
    const formatted = formatJid(customTarget);
    return formatted ? [formatted] : [];
  }
  
  const targetSet = new Set();
  if (Array.isArray(currentConfig.WA_TARGET_ROUTING) && currentConfig.WA_TARGET_ROUTING.length > 0) {
    currentConfig.WA_TARGET_ROUTING
      .filter(t => t.enabled !== false && (t.target || t.jid))
      .forEach(t => {
        const f = formatJid(t.target || t.jid);
        if (f) targetSet.add(f);
      });
  }

  if (targetSet.size === 0 && currentConfig.WA_TARGET_JID) {
    const single = formatJid(currentConfig.WA_TARGET_JID);
    if (single) targetSet.add(single);
  }

  return Array.from(targetSet);
}

// Kirim Pesan WhatsApp (Multi-Target Routing & Broadcast Support)
async function sendWhatsAppAlert(serverName, jobName, status, lastRunTime, dashboardUrl, errorDesc = '', customTargetJid = null) {
  if (!waSock || !isConnected) {
    logSystem('warning', `WhatsApp belum terhubung. Alert untuk ${serverName} - ${jobName} ditunda.`);
    return false;
  }

  const targetJids = getActiveTargetJids(customTargetJid);
  if (targetJids.length === 0) {
    logSystem('error', `Nomor atau grup target WhatsApp belum diatur. Alert untuk ${serverName} - ${jobName} dibatalkan.`);
    return false;
  }

  // Cari dashboard URL server jika kosong
  if (!dashboardUrl) {
    const srv = currentConfig.TARGET_SERVERS.find(s => s.name === serverName);
    dashboardUrl = srv ? srv.baseUrl : 'https://jobs.asiatop.co.id:3013';
  }

  let template = currentConfig.ALERT_TEMPLATE || DEFAULT_CONFIG.ALERT_TEMPLATE;
  let message = template
    .replace(/{serverName}/g, serverName)
    .replace(/{jobName}/g, jobName)
    .replace(/{status}/g, status)
    .replace(/{lastRunTime}/g, lastRunTime)
    .replace(/{dashboardUrl}/g, dashboardUrl);

  if (errorDesc) {
    message += `\n📝 *Error Detail:* ${errorDesc.substring(0, 200)}`;
  }

  let anySent = false;
  for (const targetJid of targetJids) {
    try {
      const sentMsg = await waSock.sendMessage(targetJid, { text: message });
      storeSentMessage(sentMsg);
      monitoringStats.totalAlertsSent++;
      anySent = true;
      
      const alertEntry = {
        id: Date.now().toString() + Math.random().toString(36).substring(2, 5),
        serverName,
        jobName,
        status,
        lastRunTime,
        dashboardUrl,
        errorDesc,
        targetJid,
        timestamp: new Date().toLocaleString('id-ID', { timeZone: 'Asia/Jakarta' })
      };
      monitoringStats.recentAlerts.unshift(alertEntry);
      if (monitoringStats.recentAlerts.length > 50) monitoringStats.recentAlerts.pop();

      logSystem('success', `[WA-DISPATCH] Alert terkirim ke ${targetJid} [${serverName} -> ${jobName}]`);
      await new Promise(r => setTimeout(r, 150));
    } catch (err) {
      logSystem('error', `Gagal mengirim WhatsApp alert ke ${targetJid} (${serverName} - ${jobName}): ${err.message}`);
    }
  }

  if (anySent) {
    broadcastStateToClients();
    return true;
  }
  return false;
}

// Fitur Pengiriman Summary Rekap Error Berkala (Periodic Digest Bot)
let lastSummarySentTime = 0;
let summaryCheckTimer = null;

async function sendPeriodicSummaryAlert(isManualTest = false) {
  if (!isConnected || !waSock) {
    if (isManualTest) throw new Error('WhatsApp belum terhubung! Silakan scan QR code terlebih dahulu.');
    return { success: false, message: 'WhatsApp belum terhubung' };
  }

  if (!currentConfig.SUMMARY_ENABLED && !isManualTest) {
    return { success: false, message: 'Fitur summary berkala dinonaktifkan' };
  }

  const activeErrors = (monitoringStats.activeErrors && monitoringStats.activeErrors.length > 0)
    ? monitoringStats.activeErrors
    : (monitoringStats.detectedErrors ? monitoringStats.detectedErrors.filter(e => !e.resolved) : []);

  if (currentConfig.SUMMARY_ONLY_IF_ERROR && activeErrors.length === 0 && !isManualTest) {
    return { success: false, message: 'Tidak ada error aktif saat ini (0 error)' };
  }

  const targetJids = getActiveTargetJids();
  if (targetJids.length === 0) {
    if (isManualTest) throw new Error('Target WhatsApp belum dikonfigurasi!');
    return { success: false, message: 'Target WhatsApp belum dikonfigurasi' };
  }

  const timeStr = new Date().toLocaleString('id-ID', { timeZone: 'Asia/Jakarta' });
  let errorListText = '';

  if (activeErrors.length > 0) {
    errorListText = activeErrors.map((err, idx) => {
      const num = idx + 1;
      return `*${num}. [${err.serverName}] ${err.jobName}*\n   ├ ⚠️ *Status:* Failed (Code: ${err.code})\n   ├ 🕒 *Waktu:* ${err.timeStr}\n   └ 📝 *Info:* ${err.description || 'Exit code ' + err.code}`;
    }).join('\n\n');
  } else {
    errorListText = '✅ *Semua Job Berjalan Normal (0 Error Aktif)*\nTidak ada scheduler job yang gagal saat ini.';
  }

  const firstSrv = currentConfig.TARGET_SERVERS.find(s => s.enabled !== false);
  const dashboardUrl = firstSrv ? firstSrv.baseUrl : 'https://jobs.asiatop.co.id:3013';
  const intervalStr = `${currentConfig.SUMMARY_INTERVAL_MINUTES || 30}`;

  let template = currentConfig.SUMMARY_TEMPLATE || DEFAULT_CONFIG.SUMMARY_TEMPLATE;
  let message = template
    .replace(/{timestamp}/g, timeStr)
    .replace(/{totalErrors}/g, String(activeErrors.length))
    .replace(/{errorList}/g, errorListText)
    .replace(/{dashboardUrl}/g, dashboardUrl)
    .replace(/{interval}/g, intervalStr);

  let sentList = [];
  let failList = [];

  for (const targetJid of targetJids) {
    try {
      const sentMsg = await waSock.sendMessage(targetJid, { text: message });
      storeSentMessage(sentMsg);
      monitoringStats.totalAlertsSent++;
      sentList.push(targetJid);
      logSystem('success', `[SUMMARY-BOT] Rekap summary ${activeErrors.length} job error berhasil dikirim ke ${targetJid}`);
      await new Promise(r => setTimeout(r, 200));
    } catch (err) {
      failList.push(targetJid);
      logSystem('error', `[SUMMARY-BOT] Gagal kirim summary ke ${targetJid}: ${err.message}`);
    }
  }

  if (sentList.length > 0) {
    lastSummarySentTime = Date.now();
    broadcastStateToClients();
    return { 
      success: true, 
      message: `Summary berhasil dikirim ke ${sentList.length} target (${sentList.join(', ')})!`, 
      sent: sentList, 
      failed: failList 
    };
  }
  return { success: false, message: 'Gagal mengirim summary ke seluruh target.', failed: failList };
}

// Fast Real-Time Poller (Runs every 1.5 seconds in parallel for zero delay)
async function fastPollActiveJobs() {
  if (isFastPolling) return;
  isFastPolling = true;

  const httpsAgent = new https.Agent({
    rejectUnauthorized: !currentConfig.IGNORE_SSL_ERRORS
  });

  let stateChanged = false;

  try {
    await Promise.all(currentConfig.TARGET_SERVERS.map(async (server) => {
      if (server.enabled === false) return;

      const apiKey = server.apiKey || '';
      const baseUrl = server.baseUrl.replace(/\/+$/, '');

      const activeEndpoint = `${baseUrl}/api/app/get_active_jobs${apiKey ? '?api_key=' + apiKey : ''}`;
      const statusEndpoint = `${baseUrl}/api/app/status${apiKey ? '?api_key=' + apiKey : ''}`;
      const quickHistUrl = apiKey ? `${baseUrl}/api/app/get_history?limit=30&api_key=${apiKey}` : null;

      try {
        const startTime = Date.now();
        const requests = [
          axios.get(activeEndpoint, { timeout: 3500, httpsAgent }).catch(() => null),
          axios.get(statusEndpoint, { timeout: 3500, httpsAgent }).catch(() => null)
        ];
        if (quickHistUrl) {
          requests.push(axios.get(quickHistUrl, { timeout: 3500, httpsAgent }).catch(() => null));
        }

        const [activeRes, statusRes, quickHistRes] = await Promise.all(requests);
        const latencyMs = Date.now() - startTime;

        if (statusRes && statusRes.data) {
          if (!monitoringStats.serverStatus[server.name]) {
            monitoringStats.serverStatus[server.name] = { online: true, latencyMs };
          } else {
            monitoringStats.serverStatus[server.name].online = true;
            monitoringStats.serverStatus[server.name].latencyMs = latencyMs;
          }
        }

        if (activeRes && activeRes.data && activeRes.data.jobs) {
          const activeJobsObj = activeRes.data.jobs;
          const activeJobsList = Object.keys(activeJobsObj).map(jobId => {
            const item = activeJobsObj[jobId];
            const timeStart = item.time_start || (Date.now() / 1000);
            const elapsedSec = Math.max(0, Math.floor((Date.now() / 1000) - timeStart));
            
            // Calculate or extract progress percentage
            let progressPercent = 0;
            if (typeof item.progress === 'number') {
              progressPercent = item.progress <= 1 ? Math.round(item.progress * 100) : Math.round(item.progress);
            } else if (typeof item.percent === 'number') {
              progressPercent = Math.round(item.percent);
            } else if (item.estimated_duration && item.estimated_duration > 0) {
              progressPercent = Math.min(99, Math.max(5, Math.round((elapsedSec / item.estimated_duration) * 100)));
            } else {
              progressPercent = Math.min(96, Math.max(6, Math.round(100 * (1 - Math.exp(-elapsedSec / 45)))));
            }

            return {
              id: jobId,
              serverName: server.name,
              title: item.event_title || item.title || item.event || jobId,
              category: item.category_title || item.category || '-',
              hostname: item.hostname || 'vm-jobs',
              plugin: item.plugin_title || 'Shell Script',
              timeStart,
              elapsedSec,
              elapsedFormatted,
              progressPercent,
              progress: item.progress,
              pid: item.pid || '-',
              cpu: item.cpu?.current !== undefined ? `${item.cpu.current.toFixed(1)}%` : '-',
              cpuRaw: item.cpu?.current,
              mem: item.mem?.current ? `${(item.mem.current / (1024 * 1024)).toFixed(1)} MB` : '-',
              memRaw: item.mem?.current,
              source: item.source || 'Manual/Scheduled',
              status: 'Running'
            };
          });

          const prevCount = (monitoringStats.activeJobsCache[server.name] || []).length;
          monitoringStats.activeJobsCache[server.name] = activeJobsList;
          if (prevCount !== activeJobsList.length || activeJobsList.length > 0) {
            stateChanged = true;
          }
        }

        // Fast Error Detection & Instant Auto WhatsApp Alert (Parallel Execution)
        if (quickHistRes && quickHistRes.data && quickHistRes.data.rows) {
          const sentAlertsRaw = getSentAlerts();
          const sentAlertKeys = sentAlertsRaw.map(a => typeof a === 'string' ? a : a.key);

          for (const row of quickHistRes.data.rows) {
            if (row.code !== 0) {
              const jobName = row.event_title || row.event || 'Unknown Job';
              const runTimeStr = new Date(row.time_start * 1000).toLocaleString('id-ID', { timeZone: 'Asia/Jakarta' });
              const alertKey = `${server.name}_${jobName}_${row.time_start}`;

              // Sinkronkan ke daftar Insiden Dashboard seketika
              const errId = row.id || `${row.time_start}`;
              let persistentErrors = loadPersistentErrors();
              let existingErrIdx = persistentErrors.findIndex(e => e.serverName === server.name && (e.id === errId || (e.jobName === jobName && Math.abs(e.time_start - row.time_start) < 2)));
              
              if (existingErrIdx === -1) {
                const newErrEntry = {
                  id: errId,
                  event: row.event || '',
                  serverName: server.name,
                  jobName,
                  category: row.category_title || '-',
                  code: row.code,
                  description: row.description || `Exit code ${row.code}`,
                  time_start: row.time_start,
                  timeStr: runTimeStr,
                  elapsed: row.elapsed ? Number(row.elapsed).toFixed(1) + 's' : '-',
                  resolved: false,
                  latestStatus: 'Failed'
                };
                persistentErrors.unshift(newErrEntry);
                savePersistentErrors(persistentErrors);
                monitoringStats.detectedErrors = persistentErrors;
                monitoringStats.activeErrors = persistentErrors.filter(e => !e.resolved);
                monitoringStats.recoveredErrors = persistentErrors.filter(e => e.resolved);
                stateChanged = true;
              }

              if (!sentAlertKeys.includes(alertKey)) {
                const delayMs = (currentConfig.ALERT_DELAY_MINUTES || 0) * 60 * 1000;
                const errorAgeMs = Date.now() - (row.time_start * 1000);
                if (delayMs > 0 && errorAgeMs < delayMs) {
                  // Delay belum terpenuhi, tunggu sebelum kirim
                  continue;
                }

                logSystem('warning', `⚡ [ALERT-DISPATCH] Job Gagal Terdeteksi: [${server.name}] ${jobName} (Code: ${row.code}) -> Mengirim WhatsApp...`);
                
                // Kirim alert
                const sentSuccess = await sendWhatsAppAlert(
                  server.name,
                  jobName,
                  `FAILED (Code: ${row.code})`,
                  runTimeStr,
                  server.baseUrl,
                  row.description
                );

                // Hanya tandai terkirim ke anti-spam logger JIKA berhasil dikirim ke WhatsApp
                if (sentSuccess) {
                  saveSentAlert(alertKey, {
                    serverName: server.name,
                    jobName,
                    status: `Error (Code: ${row.code})`,
                    lastRun: runTimeStr,
                    description: row.description || ''
                  });
                  sentAlertKeys.push(alertKey);
                  stateChanged = true;
                }
              }
            }
          }
        }
      } catch (err) {}
    }));
  } finally {
    isFastPolling = false;
    if (stateChanged) {
      broadcastStateToClients();
    }
  }
}

// Deep History & Schedule Scanner
async function checkJobsStatus(deep = true) {
  if (isChecking) return;

  isChecking = true;
  lastCheckTime = new Date().toLocaleString('id-ID', { timeZone: 'Asia/Jakarta' });
  monitoringStats.totalChecks++;

  const sentAlertsRaw = getSentAlerts();
  const sentAlertKeys = sentAlertsRaw.map(a => typeof a === 'string' ? a : a.key);

  const httpsAgent = new https.Agent({
    rejectUnauthorized: !currentConfig.IGNORE_SSL_ERRORS
  });

  // Load existing persistent errors
  let persistentErrors = loadPersistentErrors();
  const existingErrorMap = new Map();
  persistentErrors.forEach(err => {
    existingErrorMap.set(`${err.serverName}_${err.id}`, err);
  });

  for (const server of currentConfig.TARGET_SERVERS) {
    if (server.enabled === false) continue;

    const apiKey = server.apiKey || '';
    const baseUrl = server.baseUrl.replace(/\/+$/, '');

    try {
      const startTime = Date.now();
      
      let statusEndpoint = `${baseUrl}/api/app/status${apiKey ? '?api_key=' + apiKey : ''}`;
      let scheduleEndpoint = `${baseUrl}/api/app/get_schedule${apiKey ? '?api_key=' + apiKey : ''}`;
      let activeEndpoint = `${baseUrl}/api/app/get_active_jobs${apiKey ? '?api_key=' + apiKey : ''}`;

      // 1. Cek Status Server
      const statusRes = await axios.get(statusEndpoint, {
        timeout: 10000,
        httpsAgent,
        headers: { 'X-API-Key': apiKey }
      });
      const latencyMs = Date.now() - startTime;

      if (statusRes.data.code && statusRes.data.code === 'api') {
        throw new Error(statusRes.data.description || 'Invalid API Key');
      }

      // 2. Ambil Schedule & Active Jobs
      let scheduledJobs = [];
      let activeJobsObj = {};

      try {
        const schedRes = await axios.get(scheduleEndpoint, { timeout: 10000, httpsAgent });
        if (schedRes.data && schedRes.data.rows) scheduledJobs = schedRes.data.rows;
      } catch (e) {}

      try {
        const activeRes = await axios.get(activeEndpoint, { timeout: 10000, httpsAgent });
        if (activeRes.data && activeRes.data.jobs) activeJobsObj = activeRes.data.jobs;
      } catch (e) {}

      // 3. Multi-Page History Scanning
      let historyRows = [];
      const maxPages = (deep && apiKey) ? 5 : 2;
      for (let p = 0; p < maxPages; p++) {
        const offset = p * 1000;
        try {
          const histUrl = `${baseUrl}/api/app/get_history?limit=1000&offset=${offset}${apiKey ? '&api_key=' + apiKey : ''}`;
          const histRes = await axios.get(histUrl, { timeout: 15000, httpsAgent });
          if (histRes.data && histRes.data.rows && histRes.data.rows.length > 0) {
            historyRows = historyRows.concat(histRes.data.rows);
            if (histRes.data.rows.length < 1000) break;
          } else {
            break;
          }
        } catch (errHist) {
          break;
        }
      }

      // Format Active Jobs List
      const activeJobsList = Object.keys(activeJobsObj).map(jobId => {
        const item = activeJobsObj[jobId];
        const timeStart = item.time_start || (Date.now() / 1000);
        const elapsedSec = Math.max(0, Math.floor((Date.now() / 1000) - timeStart));
        
        let elapsedFormatted = `${elapsedSec}s`;
        if (elapsedSec >= 60) {
          const m = Math.floor(elapsedSec / 60);
          const s = elapsedSec % 60;
          elapsedFormatted = `${m}m ${s}s`;
        }

        return {
          id: jobId,
          serverName: server.name,
          title: item.event_title || item.title || item.event || jobId,
          category: item.category_title || item.category || '-',
          hostname: item.hostname || 'vm-jobs',
          plugin: item.plugin_title || 'Shell Script',
          timeStart,
          elapsedSec,
          elapsedFormatted,
          pid: item.pid || '-',
          cpu: item.cpu?.current !== undefined ? `${item.cpu.current.toFixed(1)}%` : '-',
          cpuRaw: item.cpu?.current,
          mem: item.mem?.current ? `${(item.mem.current / (1024 * 1024)).toFixed(1)} MB` : '-',
          memRaw: item.mem?.current,
          source: item.source || 'Manual/Scheduled',
          status: 'Running'
        };
      });

      monitoringStats.activeJobsCache[server.name] = activeJobsList;

      // Pemetaan riwayat terakhir per job title & Event ID
      const latestHistoryMap = {};
      const jobHasPastErrorMap = {};

      historyRows.forEach(row => {
        const title = row.event_title || row.event || 'Unknown';
        const eventId = row.event || '';

        if (!latestHistoryMap[title]) {
          latestHistoryMap[title] = row;
        }
        if (eventId && !latestHistoryMap[eventId]) {
          latestHistoryMap[eventId] = row;
        }

        if (row.code !== 0) {
          jobHasPastErrorMap[title] = true;
          if (eventId) jobHasPastErrorMap[eventId] = true;

          const errorKey = `${server.name}_${row.id}`;
          const runDate = new Date(row.time_start * 1000);
          
          const errorEntry = {
            id: row.id,
            event: row.event || '',
            serverName: server.name,
            jobName: title,
            category: row.category_title || '-',
            code: row.code,
            description: row.description || `Exit code ${row.code}`,
            time_start: row.time_start,
            timeStr: runDate.toLocaleString('id-ID', { timeZone: 'Asia/Jakarta' }),
            elapsed: row.elapsed ? Number(row.elapsed).toFixed(1) + 's' : '-',
            resolved: false,
            latestStatus: 'Failed'
          };

          existingErrorMap.set(errorKey, errorEntry);
        }
      });

      // Evaluasi status Resolved/Recovered untuk setiap error
      existingErrorMap.forEach((err, key) => {
        if (err.serverName === server.name) {
          const latestRun = latestHistoryMap[err.jobName] || latestHistoryMap[err.event];
          if (latestRun) {
            if (latestRun.code === 0 && latestRun.time_start > err.time_start) {
              err.resolved = true;
              err.latestStatus = 'Recovered (Sukses di retry)';
              err.latestRunTime = new Date(latestRun.time_start * 1000).toLocaleString('id-ID', { timeZone: 'Asia/Jakarta' });
            } else if (latestRun.code !== 0 && latestRun.time_start >= err.time_start) {
              err.resolved = false;
              err.latestStatus = `Failed (Code ${latestRun.code})`;
              err.latestRunTime = new Date(latestRun.time_start * 1000).toLocaleString('id-ID', { timeZone: 'Asia/Jakarta' });
            }
          }
          const isCurrentlyRunning = activeJobsList.some(act => act.title === err.jobName);
          if (isCurrentlyRunning) {
            err.latestStatus = 'Running';
          }
        }
      });

      // Normalisasi daftar job untuk Job Explorer
      let errorCount = 0;
      let successCount = 0;
      let runningCount = activeJobsList.length;

      const normalizedJobs = scheduledJobs.map(job => {
        const title = job.title || job.name || 'Unnamed Job';
        const eventId = job.id || '';
        const hist = latestHistoryMap[title] || latestHistoryMap[eventId];
        
        let status = 'Idle';
        let isError = false;
        let hadPastError = !!jobHasPastErrorMap[title] || !!jobHasPastErrorMap[eventId];
        let lastRun = 'Belum pernah jalan';
        let duration = '-';

        if (hist) {
          const runDate = new Date(hist.time_start * 1000);
          lastRun = runDate.toLocaleString('id-ID', { timeZone: 'Asia/Jakarta' });
          duration = hist.elapsed ? Number(hist.elapsed).toFixed(1) + 's' : '-';
          
          if (hist.code !== 0) {
            status = `Error (Code ${hist.code})`;
            isError = true;
            errorCount++;
          } else {
            status = hadPastError ? 'Recovered' : 'Success';
            successCount++;
          }
        }

        const isRunning = activeJobsList.some(act => act.title === title);
        if (isRunning) {
          status = 'Running';
        }

        return {
          id: job.id || title,
          name: title,
          category: job.category_title || job.category || '-',
          status,
          isError,
          hadPastError,
          isRunning,
          lastRun,
          duration,
          serverName: server.name,
          enabled: job.enabled !== 0
        };
      });

      monitoringStats.jobsCache[server.name] = normalizedJobs;
      monitoringStats.serverStatus[server.name] = {
        online: true,
        latencyMs,
        hostname: statusRes.data.hostname || 'Online',
        version: statusRes.data.version || '0.9.x',
        rawMem: statusRes.data.mem,
        totalJobs: normalizedJobs.length,
        errorJobsCount: errorCount,
        successCount,
        runningCount,
        activeJobs: activeJobsList,
        lastChecked: lastCheckTime
      };

      // 4. Deteksi error pada History Terbaru untuk Alert WhatsApp
      for (const row of historyRows.slice(0, 50)) {
        if (row.code !== 0) {
          const jobName = row.event_title || row.event || 'Unknown Job';
          const runTimeStr = new Date(row.time_start * 1000).toLocaleString('id-ID', { timeZone: 'Asia/Jakarta' });
          const alertKey = `${server.name}_${jobName}_${row.time_start}`;

          if (!sentAlertKeys.includes(alertKey)) {
            logSystem('warning', `🚨 ERROR BARU TERDETEKSI: [${server.name}] ${jobName} (Code: ${row.code})`);
            const sentSuccess = await sendWhatsAppAlert(
              server.name,
              jobName,
              `FAILED (Code: ${row.code})`,
              runTimeStr,
              server.baseUrl,
              row.description
            );
            if (sentSuccess) {
              saveSentAlert(alertKey, {
                serverName: server.name,
                jobName,
                status: `Error (Code: ${row.code})`,
                lastRun: runTimeStr,
                description: row.description || ''
              });
              sentAlertKeys.push(alertKey);
            }
          }
        }
      }

    } catch (error) {
      monitoringStats.serverStatus[server.name] = {
        online: false,
        lastError: error.message,
        lastChecked: lastCheckTime,
        totalJobs: 0,
        errorJobsCount: 0,
        runningCount: 0,
        activeJobs: []
      };
      monitoringStats.activeJobsCache[server.name] = [];
      logSystem('error', `Gagal fetch data dari server [${server.name}]: ${error.message}`);
    }
  }

  // Update Persistent Errors Array
  const finalErrors = Array.from(existingErrorMap.values());
  finalErrors.sort((a, b) => b.time_start - a.time_start);
  savePersistentErrors(finalErrors);

  monitoringStats.detectedErrors = finalErrors;
  monitoringStats.activeErrors = finalErrors.filter(e => !e.resolved);
  monitoringStats.recoveredErrors = finalErrors.filter(e => e.resolved);

  isChecking = false;
  broadcastStateToClients();
}

// Koneksi WhatsApp Baileys
async function connectToWhatsApp() {
  logSystem('info', 'Menginisialisasi modul WhatsApp via Baileys...');
  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const { version } = await fetchLatestBaileysVersion().catch(() => ({ version: [2, 3000, 1015901307] }));

  const msgLogger = pino({ level: 'silent' });

  waSock = makeWASocket({
    version,
    auth: state,
    logger: msgLogger,
    printQRInTerminal: false,
    generateHighQualityLinkPreview: false,
    syncFullHistory: false,
    markOnlineOnConnect: true,
    msgRetryCounterCache,
    defaultQueryTimeoutMs: 60000,
    connectTimeoutMs: 60000,
    retryRequestDelayMs: 250,
    maxMsgRetryCount: 5,
    getMessage: async (key) => {
      if (key.id && sentMessagesStore.has(key.id)) {
        return sentMessagesStore.get(key.id);
      }
      return {
        conversation: 'Chronos Scheduler Alert'
      };
    },
    browser: ['SCOPE', 'Chrome', '3.0.0']
  });

  waSock.ev.on('creds.update', saveCreds);

  waSock.ev.on('connection.update', (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      currentQrCode = qr;
      logSystem('warning', 'QR Code baru digenerate. Silakan scan via Web Dashboard.');
      qrcodeTerminal.generate(qr, { small: true });
      broadcastStateToClients();
    }

    if (connection === 'close') {
      isConnected = false;
      waUserInfo = null;
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
      
      logSystem('warning', `Koneksi WhatsApp terputus (Status Code: ${statusCode}). Reconnect: ${shouldReconnect}`);
      broadcastStateToClients();
      
      if (shouldReconnect) {
        setTimeout(() => connectToWhatsApp(), 5000);
      } else {
        logSystem('error', 'Sesi WhatsApp Logout. Silakan tautkan ulang QR code.');
      }
    } else if (connection === 'open') {
      isConnected = true;
      currentQrCode = null;
      waUserInfo = waSock.user || { name: 'SCOPE Bot' };
      logSystem('success', `WhatsApp Connected! User: ${waUserInfo.name || waUserInfo.id || 'Active'}`);
      broadcastStateToClients();
      
      checkJobsStatus(true);
    }
  });
}

// Setup Cron & Fast Polling & Summary Timer
function setupSchedulers() {
  if (cronTask) cronTask.stop();
  if (fastPollTimer) clearInterval(fastPollTimer);
  if (summaryCheckTimer) clearInterval(summaryCheckTimer);

  // Background check
  cronTask = cron.schedule(currentConfig.CRON_SCHEDULE, () => {
    checkJobsStatus(false);
  });

  // Fast Poller (Live Active Jobs ticker every 1.5 - 3 seconds)
  const pollInterval = currentConfig.FAST_POLL_INTERVAL_MS || 1500;
  fastPollTimer = setInterval(() => {
    fastPollActiveJobs();
  }, pollInterval);

  // Periodic Summary Timer (Check every 30 seconds if due)
  summaryCheckTimer = setInterval(() => {
    if (!currentConfig.SUMMARY_ENABLED) return;
    const intervalMs = (Math.max(1, currentConfig.SUMMARY_INTERVAL_MINUTES || 30)) * 60 * 1000;
    if (Date.now() - lastSummarySentTime >= intervalMs) {
      sendPeriodicSummaryAlert(false);
    }
  }, 30000);
}

// REST API Endpoints
app.post('/api/summary/send-now', async (req, res) => {
  try {
    const result = await sendPeriodicSummaryAlert(true);
    res.json(result);
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});
app.get('/api/status', (req, res) => {
  res.json(getSystemStatePayload());
});

// SSE Real-Time Streaming Endpoint
app.get('/api/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders?.();

  const client = { id: Date.now() + Math.random(), res };
  sseClients.add(client);

  res.write(`event: state\ndata: ${JSON.stringify(getSystemStatePayload())}\n\n`);

  const pingInterval = setInterval(() => {
    try {
      res.write(': heartbeat\n\n');
    } catch (e) {
      clearInterval(pingInterval);
    }
  }, 15000);

  req.on('close', () => {
    clearInterval(pingInterval);
    sseClients.delete(client);
  });
});

app.get('/api/jobs', (req, res) => {
  let allJobs = [];
  Object.keys(monitoringStats.jobsCache).forEach(serverName => {
    allJobs = allJobs.concat(monitoringStats.jobsCache[serverName]);
  });
  res.json({ total: allJobs.length, jobs: allJobs });
});

app.get('/api/active-jobs', (req, res) => {
  let allActiveJobs = [];
  Object.keys(monitoringStats.activeJobsCache).forEach(serverName => {
    allActiveJobs = allActiveJobs.concat(monitoringStats.activeJobsCache[serverName]);
  });
  res.json({ total: allActiveJobs.length, activeJobs: allActiveJobs });
});

app.get('/api/errors', (req, res) => {
  res.json({
    total: monitoringStats.detectedErrors.length,
    activeCount: monitoringStats.activeErrors.length,
    recoveredCount: monitoringStats.recoveredErrors.length,
    errors: monitoringStats.detectedErrors
  });
});

app.post('/api/errors/clear', (req, res) => {
  clearPersistentErrors();
  logSystem('info', 'Daftar riwayat error berhasil dibersihkan.');
  res.json({ success: true, message: 'Riwayat error berhasil dikosongkan.' });
});

// Kirim Manual Alert WhatsApp untuk 1 Insiden Spesifik
app.post('/api/errors/send-alert', async (req, res) => {
  if (!isConnected || !waSock) {
    return res.status(400).json({ success: false, message: 'WhatsApp belum terhubung!' });
  }

  const { errorId, serverName, jobName, code, description, timeStr, customTarget } = req.body;
  let targetErr = null;
  if (errorId) {
    const persistentErrors = loadPersistentErrors();
    targetErr = persistentErrors.find(e => e.id === errorId);
  }

  const sName = serverName || (targetErr ? targetErr.serverName : 'Server');
  const jName = jobName || (targetErr ? targetErr.jobName : 'Job');
  const cCode = code !== undefined ? code : (targetErr ? targetErr.code : 1);
  const dDesc = description || (targetErr ? targetErr.description : '');
  const tTime = timeStr || (targetErr ? targetErr.timeStr : new Date().toLocaleString('id-ID'));

  const srv = currentConfig.TARGET_SERVERS.find(s => s.name === sName);
  const baseUrl = srv ? srv.baseUrl : '';

  const sent = await sendWhatsAppAlert(
    sName,
    jName,
    `FAILED (Code: ${cCode})`,
    tTime,
    baseUrl,
    dDesc,
    customTarget
  );

  if (sent) {
    res.json({ success: true, message: `Alert untuk [${sName}] ${jName} berhasil dikirim ke WhatsApp!` });
  } else {
    res.status(500).json({ success: false, message: `Gagal mengirim alert ke WhatsApp. Pastikan nomor tujuan valid.` });
  }
});

// Kirim Batch Alert WhatsApp untuk Semua Error Aktif
app.post('/api/errors/send-all-active', async (req, res) => {
  if (!isConnected || !waSock) {
    return res.status(400).json({ success: false, message: 'WhatsApp belum terhubung!' });
  }

  const activeErrors = monitoringStats.activeErrors.length > 0 
    ? monitoringStats.activeErrors 
    : monitoringStats.detectedErrors.slice(0, 10);

  if (activeErrors.length === 0) {
    return res.json({ success: true, message: 'Tidak ada error aktif untuk dikirim.' });
  }

  // Kirim secara bertahap
  (async () => {
    for (const err of activeErrors) {
      const srv = currentConfig.TARGET_SERVERS.find(s => s.name === err.serverName);
      const baseUrl = srv ? srv.baseUrl : '';
      await sendWhatsAppAlert(
        err.serverName,
        err.jobName,
        `FAILED (Code: ${err.code})`,
        err.timeStr,
        baseUrl,
        err.description
      );
      await new Promise(r => setTimeout(r, 1200));
    }
  })();

  res.json({ 
    success: true, 
    message: `Sedang mengirim notifikasi untuk ${activeErrors.length} job gagal ke WhatsApp...` 
  });
});

app.post('/api/check-now', async (req, res) => {
  logSystem('info', 'Pengecekan manual instan diminta dari dashboard UI.');
  checkJobsStatus(true);
  res.json({ success: true, message: 'Pengecekan mendalam telah dimulai.' });
});

// Ambil Daftar Grup WhatsApp yang Diikuti Bot (Quick Group Picker)
app.get('/api/whatsapp/groups', async (req, res) => {
  if (!isConnected || !waSock) {
    return res.status(400).json({ success: false, message: 'WhatsApp bot belum terhubung!', groups: [] });
  }
  try {
    const groupsObj = await waSock.groupFetchAllParticipating();
    const groupsList = Object.values(groupsObj).map(g => ({
      id: g.id,
      subject: g.subject || 'Unnamed Group',
      size: (g.participants || []).length,
      creation: g.creation,
      owner: g.owner || g.subjectOwner
    }));
    groupsList.sort((a, b) => a.subject.localeCompare(b.subject));
    res.json({ success: true, count: groupsList.length, groups: groupsList });
  } catch (err) {
    logSystem('error', `Gagal mengambil daftar grup WhatsApp: ${err.message}`);
    res.status(500).json({ success: false, message: err.message, groups: [] });
  }
});

app.post('/api/test-whatsapp', async (req, res) => {
  if (!isConnected || !waSock) {
    return res.status(400).json({ success: false, message: 'WhatsApp belum terhubung!' });
  }

  const inputNumber = req.body?.targetNumber;
  const targets = inputNumber ? [formatJid(inputNumber)] : getActiveTargetJids();
  if (targets.length === 0) {
    return res.status(400).json({ success: false, message: 'Nomor atau grup target WhatsApp belum diisi!' });
  }

  const testMessage = `🧪 *TEST NOTIFIKASI SCOPE*\n\n✅ Kanal notifikasi WhatsApp AKTIF & BERJALAN NORMAL!\n📅 Waktu Test: ${new Date().toLocaleString('id-ID', { timeZone: 'Asia/Jakarta' })}\n🤖 Engine: SCOPE (System Control, Operations & Performance Engine)\n\n_Setiap ada job gagal akan otomatis masuk ke penerima ini._`;
  
  let sentList = [];
  let failList = [];

  for (const jid of targets) {
    try {
      const sentMsg = await waSock.sendMessage(jid, { text: testMessage });
      storeSentMessage(sentMsg);
      sentList.push(jid);
      await new Promise(r => setTimeout(r, 150));
    } catch (err) {
      failList.push({ jid, error: err.message });
    }
  }

  if (sentList.length > 0) {
    logSystem('success', `Pesan uji coba terkirim ke: ${sentList.join(', ')}`);
    res.json({ 
      success: true, 
      message: `Pesan test berhasil dikirim ke ${sentList.length} target (${sentList.join(', ')})`,
      sent: sentList, 
      failed: failList 
    });
  } else {
    res.status(500).json({ success: false, message: `Gagal mengirim ke target: ${failList.map(f => f.error).join(', ')}` });
  }
});

app.post('/api/whatsapp/reset', async (req, res) => {
  try {
    if (waSock) {
      try { waSock.end(new Error('Reset requested')); } catch(e) {}
    }
    isConnected = false;
    waUserInfo = null;
    currentQrCode = null;
    if (fs.existsSync(AUTH_DIR)) {
      fs.rmSync(AUTH_DIR, { recursive: true, force: true });
    }
    logSystem('warning', 'Sesi WhatsApp direset. Memulai sesi baru untuk scan QR...');
    setTimeout(() => connectToWhatsApp(), 1200);
    broadcastStateToClients();
    res.json({ success: true, message: 'Sesi WhatsApp berhasil direset. Silakan scan ulang QR code.' });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

app.get('/api/logs', (req, res) => {
  const alerts = getSentAlerts();
  res.json({ count: alerts.length, alerts, systemLogs: monitoringStats.systemLogs });
});

app.post('/api/logs/clear', (req, res) => {
  clearSentAlerts();
  logSystem('info', 'Riwayat log anti-spam berhasil dibersihkan.');
  res.json({ success: true, message: 'Log alert berhasil dikosongkan.' });
});

app.post('/api/config', (req, res) => {
  try {
    const updated = saveConfig(req.body);
    setupSchedulers();
    logSystem('success', 'Konfigurasi sistem berhasil diperbarui.');
    res.json({ success: true, config: updated });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

// Serve frontend SPA
app.get('/', (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'index.html'));
});

// Start Cron & Background Server
ensureLogFile();
connectToWhatsApp();
setupSchedulers();

const PORT = currentConfig.PORT || 3000;
app.listen(PORT, () => {
  logSystem('success', `SCOPE — System Control, Operations & Performance Engine aktif di http://localhost:${PORT}`);
});

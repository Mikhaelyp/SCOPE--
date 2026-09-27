const { default: makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion } = require('@whiskeysockets/baileys');
const pino = require('pino');
const path = require('path');
const fs = require('fs');

async function test() {
  const AUTH_DIR = path.join(__dirname, 'auth_info_baileys');
  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const { version } = await fetchLatestBaileysVersion().catch(() => ({ version: [2, 3000, 1015901307] }));

  const sock = makeWASocket({
    version,
    auth: state,
    logger: pino({ level: 'debug' }),
    printQRInTerminal: false
  });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection } = update;
    console.log('Connection update:', connection);
    if (connection === 'open') {
      console.log('User:', sock.user);
      const config = JSON.parse(fs.readFileSync('./config.json', 'utf8'));
      console.log('Target JID in config:', config.WA_TARGET_JID);
      
      // Try sending to the bot's own number or target JID
      const ownJid = sock.user.id.split(':')[0] + '@s.whatsapp.net';
      console.log('Bot own JID:', ownJid);

      try {
        console.log('Sending message to config target:', config.WA_TARGET_JID);
        const res = await sock.sendMessage(config.WA_TARGET_JID, { text: '🧪 Test Message from Monitoring Eska to Config Target' });
        console.log('Send result to config target:', res);
      } catch (e) {
        console.error('Send error to config target:', e);
      }

      try {
        console.log('Sending message to own JID:', ownJid);
        const res2 = await sock.sendMessage(ownJid, { text: '🧪 Test Message from Monitoring Eska to Self' });
        console.log('Send result to own JID:', res2);
      } catch (e2) {
        console.error('Send error to own JID:', e2);
      }

      setTimeout(() => process.exit(0), 5000);
    }
  });
}

test();

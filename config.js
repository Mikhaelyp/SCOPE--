module.exports = {
  // Interval pengecekan (Cron syntax: tiap 1 menit)
  CRON_SCHEDULE: '* * * * *',

  // Target Server yang dimonitor
  TARGET_SERVERS: [
    {
      name: 'JOBS 3013',
      baseUrl: 'https://jobs.asiatop.co.id:3013',
      apiUrl: 'https://jobs.asiatop.co.id:3013/api/app/get_schedule',
      apiKey: 'e495e84af0472ba2f71ebe19ffbdb005'
    },
    {
      name: 'JOBS 3012',
      baseUrl: 'https://sfa.asiatop.co.id:3012',
      apiUrl: 'https://sfa.asiatop.co.id:3012/api/app/get_schedule',
      apiKey: ''
    }
  ],

  // Tujuan Notifikasi WhatsApp
  WA_TARGET_JID: '6281234567890@s.whatsapp.net',
  IGNORE_SSL_ERRORS: true,
  PORT: process.env.PORT || 3000
};

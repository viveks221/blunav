import BaseProvider from './BaseProvider.js';
import logger from '../logger/index.js';
import https from 'https';

class EmailProvider extends BaseProvider {
  constructor() {
    super('resend-email');
    this.apiKey = null;
    this.ready = false;
  }

  async initClient() {
    if (this.ready) return;

    this.apiKey = process.env.RESEND_API_KEY;
    if (!this.apiKey) {
      logger.warn('RESEND_API_KEY not configured - email provider disabled');
      return;
    }

    this.ready = true;
    logger.info('EmailProvider initialized with Resend (HTTP)', { ready: this.ready });
  }

  /** Send email using direct HTTP request */
  async send({ to, subject, body, html }) {
    if (!this.ready) await this.initClient();
    if (!this.apiKey) throw new Error('Email provider not configured');

    const from = process.env.EMAIL_FROM || 'onboarding@resend.dev';
    logger.info('EmailProvider sending email', { from, to, subject });

    const payload = JSON.stringify({ from, to, subject, text: body, html });

    return new Promise((resolve, reject) => {
      const options = {
        hostname: 'api.resend.com',
        path: '/emails',
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${this.apiKey}`,
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload),
        },
      };

      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', (chunk) => (data += chunk));
        res.on('end', () => {
          try {
            const result = JSON.parse(data);
            logger.info('Resend response', { result: data });

            if (result.error) {
              logger.error('Resend error', { error: result.error });
              reject(new Error(result.error.message || 'Resend API error'));
            } else {
              logger.info('Email sent via Resend', { id: result.id });
              resolve({ ok: true, id: result.id });
            }
          } catch (e) {
            logger.error('Failed to parse Resend response', { error: e.message, data });
            reject(e);
          }
        });
      });

      req.on('error', (e) => {
        logger.error('Resend request error', { error: e.message });
        reject(e);
      });

      req.write(payload);
      req.end();
    });
  }
}

export default EmailProvider;

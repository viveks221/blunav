import http from 'http';
import express from 'express';
import logger from '../logger/index.js';
import config from '../config/index.js';
import { publishOnce } from '../workers/outboxPublisher.js';
import { disconnectProducer } from '../queue/producer.js';
import { connectToDatabase } from '../db/connection.js';
import { runMigrations } from '../db/migrate.js';

import notifications from './routes/notifications.js';

function createApp() {
  const app = express();
  app.use(express.json());
  // Request logging middleware — logs method, path, idempotency key and body
  app.use((req, _res, next) => {
    try {
      const idem = req.get && req.get('idempotency-key');
      logger.debug('Incoming request', { method: req.method, url: req.originalUrl, idempotencyKey: idem });
    } catch (e) {
      /* noop */
    }
    next();
  });

  app.get('/health', (req, res) => res.json({ status: 'ok' }));

  app.use('/api/v1/notifications', notifications);

  app.use((err, req, res, _next) => {
    logger.error('Unhandled error', { err });
    res.status(500).json({ error: 'internal_error' });
  });

  return app;
}

let httpServer;
let outboxTimer;
let shuttingDown = false;

async function start() {
  const app = createApp();
  const port = config.port;
  httpServer = http.createServer(app);
  httpServer.listen(port, async () => {
    try {
      await connectToDatabase(); // Connect to the database
      logger.info('Database connection established');
      try {
        logger.info('Running DB migrations');
        await runMigrations();
        logger.info('DB migrations complete');
      } catch (mErr) {
        logger.error('Failed to run migrations', { err: mErr });
        process.exit(1);
      }
    } catch (err) {
      logger.error('Failed to connect to the database', { err: err});
      process.exit(1);
    }
  });




  const shutdown = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info('API graceful shutdown', { signal });
    if (outboxTimer) {
      clearInterval(outboxTimer);
      outboxTimer = null;
    }
    await new Promise((resolve) => {
      if (!httpServer) return resolve();
      httpServer.close(() => resolve());
    });
    try {
      await disconnectProducer();
    } catch {
      /* producer may never have connected */
    }
    process.exit(0);
  };

  process.once('SIGTERM', () => { shutdown('SIGTERM'); });
  process.once('SIGINT', () => { shutdown('SIGINT'); });
}

export default { createApp, start };

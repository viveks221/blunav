import express from 'express';
import logger from '../logger/index.js';
import config from '../config/index.js';

import notifications from './routes/notifications.js';

function createApp() {
  const app = express();
  app.use(express.json());

  app.get('/health', (req, res) => res.json({ status: 'ok' }));

  app.use('/api/v1/notifications', notifications);

  app.use((err, req, res, next) => {
    logger.error('Unhandled error', { err });
    res.status(500).json({ error: 'internal_error' });
  });

  return app;
}

function start() {
  const app = createApp();
  const port = config.port;
  app.listen(port, () => {
    logger.info('API server listening', { port });
  });
}

export default { createApp, start };

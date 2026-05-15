import { createLogger, format, transports } from 'winston';
const { combine, timestamp, json } = format;
import config from '../config/index.js';

const logger = createLogger({
  level: config.logLevel,
  format: combine(timestamp(), json()),
  transports: [new transports.Console({ stderrLevels: ['error'] })],
});

export default logger;

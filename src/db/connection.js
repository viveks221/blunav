import { Sequelize } from 'sequelize';
import config from '../config/index.js';
import logger from '../logger/index.js';

const sequelize = new Sequelize(config.databaseUrl, {
  logging: msg => logger.debug(msg),
  dialectOptions: { connectTimeout: 60000 },
});

export default sequelize;

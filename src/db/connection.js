import { Sequelize } from "sequelize";
import config from "../config/index.js";
import logger from "../logger/index.js";

let client;

const commonOptions = {
  dialect: 'postgres',
  dialectOptions: {
    keepAlive: true,
  },
  pool: {
    max: 50,
    min: 4,
    acquire: 30000,
    idle: 10000,
    evict: 15000,
  },
  retry: {
    max: 3,
  },
  logging: (msg) => logger.debug(msg),
};

let sequelize;
if (process.env.DATABASE_URL) {
  sequelize = new Sequelize(process.env.DATABASE_URL, commonOptions);
} else {
  sequelize = new Sequelize(
    process.env.DATABASE_NAME,
    process.env.DATABASE_USER,
    process.env.DATABASE_PASSWORD,
    {
      ...commonOptions,
      host: process.env.DATABASE_HOST || 'localhost',
      port: process.env.DATABASE_PORT || 5432,
      ssl: process.env.NODE_ENV === 'local' ? false : true,
    },
  );
}
export async function connectToDatabase() {
  try {
    await sequelize.authenticate();
    console.log("Connected to the database");
  } catch (error) {
    console.error("Unable to connect to the database:", error);
    throw error;
  }
}

export async function disconnectFromDatabase() {
  try {
    await sequelize.close();
    console.log("Disconnected from the database");
  } catch (error) {
    console.error("Unable to disconnect from the database:", error);
    throw error;
  }
}

export default sequelize;

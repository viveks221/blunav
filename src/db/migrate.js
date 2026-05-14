import path from 'path';
import { Umzug, SequelizeStorage } from 'umzug';
import sequelizeModule from './connection.js';
import Sequelize from 'sequelize';
import { fileURLToPath, pathToFileURL } from 'url';

const sequelize = sequelizeModule.default || sequelizeModule;

async function runMigrations() {
  const umzug = new Umzug({
    migrations: {
      glob: path.join(path.dirname(fileURLToPath(import.meta.url)), 'migrations/*.js'),
      resolve: async ({ name, path: migrationPath }) => {
        // dynamic import for ESM migrations
        const migration = await import(pathToFileURL(migrationPath).href).then(m => m.default || m);
        return {
          name,
          up: async () => migration.up(sequelize.getQueryInterface(), Sequelize),
          down: async () => migration.down(sequelize.getQueryInterface(), Sequelize),
        };
      },
    },
    context: sequelize.getQueryInterface(),
    storage: new SequelizeStorage({ sequelize }),
    logger: console,
  });

  try {
    await sequelize.authenticate();
    console.log('Database connected, running migrations...');
    const executed = await umzug.up();
    console.log('Migrations applied:', executed.map(m => m.name));
    process.exit(0);
  } catch (err) {
    console.error('Migration failed', err);
    process.exit(1);
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  runMigrations();
}

export { runMigrations };


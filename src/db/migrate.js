import path from 'path';
import fs from 'fs';
import { Umzug, SequelizeStorage } from 'umzug';
import sequelizeModule from './connection.js';
import Sequelize from 'sequelize';
import { fileURLToPath, pathToFileURL } from 'url';

const sequelize = sequelizeModule.default || sequelizeModule;
const __dirname = path.dirname(fileURLToPath(import.meta.url));

/**
 * Umzug v3's glob + async `resolve` is broken: the internal `paths.map` does not await
 * the resolver, so `{ ...asyncResolver() }` spreads a Promise and drops `up`/`down`.
 * We load migrations explicitly with `await import(...)`.
 */
async function buildMigrationList() {
  const migrationsDir = path.join(__dirname, 'migrations');
  const files = fs.readdirSync(migrationsDir).filter((f) => f.endsWith('.js')).sort();
  const qi = sequelize.getQueryInterface();

  const list = [];
  for (const file of files) {
    const filepath = path.join(migrationsDir, file);
    const name = file;
    const mod = await import(pathToFileURL(filepath).href);
    const upFn = mod.up;
    const downFn = mod.down;
    if (typeof upFn !== 'function') {
      throw new Error(`Migration ${filepath} must export async function up`);
    }
    list.push({
      name,
      path: filepath,
      up: async () => upFn(qi, Sequelize),
      down: typeof downFn === 'function' ? async () => downFn(qi, Sequelize) : undefined,
    });
  }
  return list;
}

async function runMigrations() {
  const umzug = new Umzug({
    migrations: () => buildMigrationList(),
    context: sequelize.getQueryInterface(),
    storage: new SequelizeStorage({ sequelize }),
    logger: console,
  });

  try {
    await sequelize.authenticate();
    console.log('Database connected, running migrations...');
    const executed = await umzug.up();
    console.log('Migrations applied:', executed.map(m => m.name));
  } catch (err) {
    console.error('Migration failed', err);
    process.exit(1);
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  runMigrations();
}

export { runMigrations };

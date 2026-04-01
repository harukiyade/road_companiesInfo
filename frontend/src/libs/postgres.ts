import { Pool } from "pg";

const globalForPg = globalThis as unknown as { pgPool?: Pool };

export const pgPool =
  globalForPg.pgPool ??
  new Pool({
    host: process.env.POSTGRES_HOST,
    port: process.env.POSTGRES_PORT ? Number(process.env.POSTGRES_PORT) : 5432,
    user: process.env.POSTGRES_USER,
    password: process.env.POSTGRES_PASSWORD,
    database: process.env.POSTGRES_DB,
    ssl: process.env.POSTGRES_SSLMODE === "require" ? { rejectUnauthorized: false } : undefined,
    max: 10,
  });

if (process.env.NODE_ENV !== "production") {
  globalForPg.pgPool = pgPool;
}


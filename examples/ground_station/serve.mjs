// A static server for web/, for the browser tests: module scripts and
// workers need a real origin with the right MIME types, and OPFS needs a
// secure context, which 127.0.0.1 is.

import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { execSync } from 'node:child_process';
import { join, extname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = fileURLToPath(new URL('./web/', import.meta.url));
const types = {
  '.html': 'text/html',
  '.css': 'text/css',
  '.mjs': 'text/javascript',
  '.wasm': 'application/wasm',
};

/** Serves web/ on a free port; resolves to `{ url, close }`. */
export async function serve() {
  const server = createServer(async (req, res) => {
    const path = new URL(req.url, 'http://x').pathname.replace(/^\/$/, '/index.html');
    try {
      const body = await readFile(join(root, path.slice(1)));
      res.writeHead(200, { 'content-type': types[extname(path)] ?? 'application/octet-stream' });
      res.end(req.method === 'HEAD' ? undefined : body);
    } catch {
      res.writeHead(404).end();
    }
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  return { url: `http://127.0.0.1:${server.address().port}/`, close: () => server.close() };
}

/** Playwright's chromium: a local install, or the global one. */
export async function chromium() {
  const pw = await import('playwright').catch(() =>
    import(pathToFileURL(join(execSync('npm root -g').toString().trim(), 'playwright', 'index.mjs')).href),
  );
  return pw.chromium;
}

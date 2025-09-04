// index.js — Cloudflare Worker (module syntax)
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const parts = url.pathname.replace(/^\/+/, "").split("/");
    const corsOrigins = env.CORS_ORIGINS || "*";

    // CORS preflight
    if (req.method === "OPTIONS") {
      return cors(new Response(null, { status: 204 }), corsOrigins);
    }

    // /latest/<SKU>[.<ext>]
    if (parts[0] === "latest" && parts[1]) {
      const last = parts[1];
      const m = last.match(/^([^\.]+?)(\.(csv|pdf|sha256))?$/i);
      if (!m) return cors(new Response("Bad SKU", { status: 400 }), corsOrigins);
      const sku = m[1];
      const ext = m[2] || ""; // ".csv" | ".pdf" | ".sha256" | ""
      const prefix = `${sku}/`; // on attend des clés R2 sous "SKU/YYYY-MM-DD.ext"
      const key = await latestByPrefix(env, prefix, ext || undefined);
      if (!key) return cors(new Response("Not Found", { status: 404 }), corsOrigins);

      const obj = await env.R2.get(key);
      if (!obj) return cors(new Response("Gone", { status: 410 }), corsOrigins);
      const type = guessType(key);
      return cors(new Response(obj.body, { headers: { "Content-Type": type } }), corsOrigins);
    }

    // /versions/<SKU>
    if (parts[0] === "versions" && parts[1]) {
      const sku = parts[1];
      const list = await env.R2.list({ prefix: `${sku}/`, limit: 1000 });
      const items = list.objects.map(o => ({ key: o.key, size: o.size, uploaded: o.uploaded }));
      return cors(new Response(JSON.stringify(items, null, 2), { headers: { "Content-Type": "application/json" } }), corsOrigins);
    }

    // health
    if (url.pathname === "/") {
      return cors(new Response("enerflux-latest worker ok"), corsOrigins);
    }

    return cors(new Response("Not Found", { status: 404 }), corsOrigins);
  }
};

function cors(resp, origins) {
  const r = new Response(resp.body, resp);
  r.headers.set("Access-Control-Allow-Origin", origins || "*");
  r.headers.set("Access-Control-Allow-Methods", "GET, OPTIONS");
  r.headers.set("Access-Control-Allow-Headers", "Content-Type");
  r.headers.set("Cache-Control", "public, max-age=60");
  return r;
}

function guessType(key) {
  if (key.endsWith(".csv")) return "text/csv; charset=utf-8";
  if (key.endsWith(".pdf")) return "application/pdf";
  if (key.endsWith(".sha256")) return "text/plain; charset=utf-8";
  if (key.endsWith(".xlsx")) return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
  return "application/octet-stream";
}

async function latestByPrefix(env, prefix, ext) {
  // Récupère la clé lexicographiquement la plus “grande” (dates ISO triables)
  let cursor = undefined;
  let best = null;
  do {
    const res = await env.R2.list({ prefix, cursor, limit: 1000 });
    for (const obj of res.objects) {
      const k = obj.key;
      if (ext && !k.endsWith(ext)) continue;
      if (!best || k > best) best = k;
    }
    cursor = res.truncated ? res.cursor : undefined;
  } while (cursor);
  return best;
}

// Deal Hunter — relais de fetch Cloudflare Worker.
//
// Exécute les requêtes des connecteurs depuis l'edge Cloudflare (IP non-datacenter,
// utile pour les sources qui bloquent les IP de VPS). Sécurisé par :
//   - un secret partagé (header X-Relay-Secret == env.RELAY_SECRET) ;
//   - une allowlist d'hôtes (env.ALLOWED_HOSTS, séparés par des virgules)
//     pour ne PAS être un proxy ouvert.
//
// Contrat : {GET|POST} /fetch?url=<URL cible encodée>
//   La méthode, le corps et les headers (hors hop-by-hop + secret) sont rejoués
//   vers la cible ; la réponse upstream est renvoyée telle quelle.

export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    if (url.pathname === "/" || url.pathname === "") {
      return new Response("deal-hunter relay ok", { status: 200 });
    }
    if (url.pathname !== "/fetch") {
      return new Response("Not Found", { status: 404 });
    }

    // 1) Authentification par secret partagé.
    const secret = env.RELAY_SECRET || "";
    if (!secret || req.headers.get("X-Relay-Secret") !== secret) {
      return new Response("Forbidden", { status: 403 });
    }

    // 2) Cible.
    const target = url.searchParams.get("url");
    if (!target) return new Response("Missing url", { status: 400 });
    let targetUrl;
    try {
      targetUrl = new URL(target);
    } catch {
      return new Response("Bad url", { status: 400 });
    }
    if (targetUrl.protocol !== "https:" && targetUrl.protocol !== "http:") {
      return new Response("Bad scheme", { status: 400 });
    }

    // 3) Allowlist d'hôtes (anti proxy ouvert).
    const allowed = (env.ALLOWED_HOSTS || "")
      .split(",")
      .map((h) => h.trim().toLowerCase())
      .filter(Boolean);
    const host = targetUrl.hostname.toLowerCase();
    const hostOk = allowed.some((a) => host === a || host.endsWith("." + a));
    if (allowed.length && !hostOk) {
      return new Response(`Host not allowed: ${host}`, { status: 403 });
    }

    // 4) Rejoue la requête vers la cible (headers hop-by-hop retirés).
    const fwd = new Headers(req.headers);
    fwd.delete("X-Relay-Secret");
    fwd.delete("Host");
    fwd.delete("Content-Length");
    fwd.delete("CF-Connecting-IP");

    const init = { method: req.method, headers: fwd, redirect: "follow" };
    if (req.method !== "GET" && req.method !== "HEAD") {
      init.body = await req.arrayBuffer();
    }

    let upstream;
    try {
      upstream = await fetch(targetUrl.toString(), init);
    } catch (e) {
      return new Response(`Upstream error: ${e}`, { status: 502 });
    }

    // 5) Renvoie la réponse upstream (statut + corps + content-type).
    const respHeaders = new Headers();
    const ct = upstream.headers.get("Content-Type");
    if (ct) respHeaders.set("Content-Type", ct);
    respHeaders.set("X-Relay-Upstream-Status", String(upstream.status));
    return new Response(upstream.body, { status: upstream.status, headers: respHeaders });
  },
};

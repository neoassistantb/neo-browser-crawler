"use strict";

import http from "http";
import crypto from "crypto";
import fs from "fs";
import path from "path";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

// Security
const CRAWLER_SECRET = process.env.CRAWLER_SECRET || "";
const CRAWLER_TOKEN = process.env.CRAWLER_TOKEN || "";

// OCR (optional)
const GOOGLE_VISION_API_KEY = process.env.GOOGLE_VISION_API_KEY || "";

// ================= LIMITS =================
// ✅ safe defaults for Render Starter (512MB). You can override via env.
const MAX_SECONDS = Number(process.env.MAX_SECONDS || 60);
const MIN_WORDS = Number(process.env.MIN_WORDS || 20);

const PARALLEL_TABS = Number(process.env.PARALLEL_TABS || 1);
const PARALLEL_OCR = Number(process.env.PARALLEL_OCR || 2);
const OCR_TIMEOUT_MS = Number(process.env.OCR_TIMEOUT_MS || 5000);

const MAX_PAGES = Number(process.env.MAX_PAGES || 40);
const MAX_QUEUE = Number(process.env.MAX_QUEUE || 600);

// For memory safety: cap each page content
const MAX_CONTENT_CHARS = Number(process.env.MAX_CONTENT_CHARS || 60000);
const MAX_OCR_CACHE = Number(process.env.MAX_OCR_CACHE || 250);

// Streaming storage (low memory)
const JOB_TTL_MS = 15 * 60 * 1000;
const jobs = new Map();

// Skip patterns
const SKIP_URL_RE =
  /(wp-content\/uploads|media|gallery|video|photo|attachment|privacy|terms|cookies|gdpr)/i;
const SKIP_OCR_RE = /\/(logo|favicon|spinner|avatar|pixel|spacer|blank|transparent)\.|\/icons?\//i;

console.log(`[BOOT] crawler ESM starting... PORT=${PORT}`);
console.log(
  `[BOOT] limits: MAX_SECONDS=${MAX_SECONDS} MIN_WORDS=${MIN_WORDS} PARALLEL_TABS=${PARALLEL_TABS} PARALLEL_OCR=${PARALLEL_OCR} MAX_PAGES=${MAX_PAGES} MAX_QUEUE=${MAX_QUEUE}`,
);
console.log(
  `[BOOT] auth: CRAWLER_SECRET=${CRAWLER_SECRET ? "set" : "unset"} CRAWLER_TOKEN=${CRAWLER_TOKEN ? "set" : "unset"} GOOGLE_VISION_API_KEY=${GOOGLE_VISION_API_KEY ? "set" : "unset"}`,
);

// ✅ Always print memory to understand OOM in Render
setInterval(() => {
  const m = process.memoryUsage();
  console.log(
    `[MEM] rss=${Math.round(m.rss / 1024 / 1024)}MB heapUsed=${Math.round(m.heapUsed / 1024 / 1024)}MB ext=${Math.round(m.external / 1024 / 1024)}MB`,
  );
}, 10_000);

// ================= CORS =================
function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "content-type, authorization, x-request-id, x-crawler-token",
  };
}

// ================= UTILS =================
const clean = (t = "") =>
  String(t)
    .replace(/\r/g, "")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

const countWordsExact = (t = "") => (String(t).match(/[A-Za-zА-Яа-я0-9]+/g) || []).length;

function getReqId(req) {
  const v = req.headers["x-request-id"];
  if (typeof v === "string" && v.trim()) return v.trim();
  return crypto.randomUUID();
}

function getPath(req) {
  try {
    const u = new URL(req.url || "/", "http://localhost");
    return u.pathname;
  } catch {
    return "/";
  }
}

function getQuery(req) {
  try {
    const u = new URL(req.url || "/", "http://localhost");
    return u.searchParams;
  } catch {
    return new URLSearchParams();
  }
}

async function readBody(req) {
  return await new Promise((resolve) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => resolve(data));
  });
}

function json(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    ...corsHeaders(),
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
  });
  res.end(body);
}

function normalizeUrl(u) {
  try {
    const url = new URL(u);
    url.hash = "";
    if (url.pathname !== "/" && url.pathname.endsWith("/")) url.pathname = url.pathname.slice(0, -1);
    return url.toString();
  } catch {
    return u;
  }
}

function capText(s, maxChars) {
  const str = String(s || "");
  if (str.length <= maxChars) return str;
  return str.slice(0, maxChars) + "\n\n[TRUNCATED]";
}

function pushQueue(queue, item) {
  if (queue.length >= MAX_QUEUE) return;
  queue.push(item);
}

function jobFilePath(jobId) {
  return path.join("/tmp", `neo_crawl_${jobId}.ndjson`);
}

// ================= AUTH =================
function checkAuth(req, reqId) {
  if (!CRAWLER_SECRET && !CRAWLER_TOKEN) return true;

  const auth = req.headers["authorization"];
  const tokenHeader = req.headers["x-crawler-token"];

  const bearer =
    typeof auth === "string" && auth.toLowerCase().startsWith("bearer ")
      ? auth.slice(7).trim()
      : "";

  const token = typeof tokenHeader === "string" ? tokenHeader.trim() : "";

  const ok =
    (CRAWLER_SECRET && bearer === CRAWLER_SECRET) ||
    (CRAWLER_TOKEN && (bearer === CRAWLER_TOKEN || token === CRAWLER_TOKEN));

  if (!ok) {
    console.log(`[AUTH] ${reqId} Unauthorized (hasAuthHeader=${!!auth} hasTokenHeader=${!!tokenHeader})`);
  }
  return ok;
}

// ================= OCR =================
async function ocrImageUrl(imageUrl, ocrCache) {
  if (!GOOGLE_VISION_API_KEY) return "";

  try {
    if (ocrCache.has(imageUrl)) return ocrCache.get(imageUrl) || "";

    const body = {
      requests: [{ image: { source: { imageUri: imageUrl } }, features: [{ type: "TEXT_DETECTION" }] }],
    };

    const r = await fetch(`https://vision.googleapis.com/v1/images:annotate?key=${GOOGLE_VISION_API_KEY}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    const data = await r.json().catch(() => null);
    const text =
      data?.responses?.[0]?.fullTextAnnotation?.text ||
      data?.responses?.[0]?.textAnnotations?.[0]?.description ||
      "";

    if (ocrCache.size >= MAX_OCR_CACHE) ocrCache.clear();
    ocrCache.set(imageUrl, text || "");
    return text || "";
  } catch {
    return "";
  }
}

async function ocrAllImages(page, stats, ocrCache) {
  try {
    const imgElements = await page.$$eval("img", (imgs) =>
      imgs
        .map((img) => ({
          src: img.currentSrc || img.src || img.getAttribute("src") || "",
          w: img.naturalWidth || img.width || 0,
          h: img.naturalHeight || img.height || 0,
        }))
        .filter((x) => x.src),
    );

    const validImages = imgElements.filter((img) => {
      if (!img.src) return false;
      if (SKIP_OCR_RE.test(img.src)) return false;
      if (img.w < 120 || img.h < 80) return false; // ✅ stricter
      return true;
    });

    const results = [];
    let idx = 0;

    while (idx < validImages.length) {
      const batch = validImages.slice(idx, idx + PARALLEL_OCR);
      idx += PARALLEL_OCR;

      const batchPromises = batch.map(async (img) => {
        try {
          const p = Promise.race([
            ocrImageUrl(img.src, ocrCache),
            new Promise((resolve) => setTimeout(() => resolve(""), OCR_TIMEOUT_MS)),
          ]);
          const text = await p;
          if (text && String(text).trim()) {
            stats.ocrElementsProcessed++;
            stats.ocrCharsExtracted += text.length;
            return text;
          }
        } catch {}
        return "";
      });

      const batchResults = await Promise.all(batchPromises);
      for (const t of batchResults) if (t) results.push(t);
    }

    return results;
  } catch {
    return [];
  }
}

// ================= HEADER/FOOTER DEDUP CORE =================
function normalizeLines(text) {
  return clean(text)
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l.length >= 4);
}

function buildShellSignature(headerText, footerText) {
  const lines = [...normalizeLines(headerText), ...normalizeLines(footerText)];
  const sig = new Set();
  for (const l of lines) {
    const s = l.replace(/\s+/g, " ").trim();
    if (!s) continue;
    if (s.length < 6) continue;
    if (s.length > 120) continue;
    if (/^[0-9\s\-\+]+$/.test(s)) continue;
    sig.add(s.toLowerCase());
  }
  return sig;
}

function removeShellFromBody(bodyText, shellSig) {
  const bodyLines = normalizeLines(bodyText);
  const kept = [];
  for (const l of bodyLines) {
    const key = l.replace(/\s+/g, " ").trim().toLowerCase();
    if (shellSig.has(key)) continue;
    kept.push(l);
  }
  const out = [];
  let prev = "";
  for (const l of kept) {
    const k = l.toLowerCase();
    if (k === prev) continue;
    out.push(l);
    prev = k;
  }
  return out.join("\n");
}

async function getHeaderFooterText(page) {
  try {
    return await page.evaluate(() => {
      const header = document.querySelector("header");
      const footer = document.querySelector("footer");
      return {
        headerText: header ? header.innerText || "" : "",
        footerText: footer ? footer.innerText || "" : "",
      };
    });
  } catch {
    return { headerText: "", footerText: "" };
  }
}

async function collectAllLinks(page, base) {
  try {
    return await page.evaluate((baseOrigin) => {
      const urls = new Set();
      document.querySelectorAll("a[href]").forEach((a) => {
        try {
          const u = new URL(a.href, baseOrigin);
          if (u.origin === baseOrigin) urls.add(u.href.split("#")[0]);
        } catch {}
      });
      return Array.from(urls);
    }, base);
  } catch {
    return [];
  }
}

async function extractMeta(page) {
  try {
    return await page.evaluate(() => {
      const get = (sel) => document.querySelector(sel)?.getAttribute("content") || "";
      const desc = get('meta[name="description"]') || get('meta[property="og:description"]');
      const ogTitle = get('meta[property="og:title"]');
      const canonical = document.querySelector('link[rel="canonical"]')?.getAttribute("href") || "";
      const lang = document.documentElement?.lang || "";
      const h = Array.from(document.querySelectorAll("h1, h2, h3"))
        .slice(0, 40)
        .map((x) => (x.innerText || "").trim())
        .filter(Boolean);
      return { desc, ogTitle, canonical, lang, headings: h };
    });
  } catch {
    return { desc: "", ogTitle: "", canonical: "", lang: "", headings: [] };
  }
}

// ================= REQUEST BLOCKING (speed + memory) =================
function shouldBlockRequest(url, resourceType) {
  const u = String(url || "").toLowerCase();
  if (resourceType === "media" || resourceType === "font") return true;
  if (resourceType === "image") return false; // allow images for OCR selection; still filtered by size/regex
  if (u.endsWith(".mp4") || u.endsWith(".mov") || u.endsWith(".avi") || u.endsWith(".webm")) return true;

  // ✅ block heavy trackers/ad networks
  if (
    u.includes("doubleclick") ||
    u.includes("googlesyndication") ||
    u.includes("googletagmanager") ||
    u.includes("google-analytics") ||
    u.includes("facebook") ||
    u.includes("tiktok") ||
    u.includes("hotjar") ||
    u.includes("clarity.ms")
  ) return true;

  return false;
}

// ================= PROCESS PAGE =================
async function processPage(page, url, base, shellSig, stats, ocrCache) {
  const startTime = Date.now();
  try {
    console.log("[PAGE]", url);

    await page.goto(url, { timeout: 15000, waitUntil: "domcontentloaded" });

    // light scroll for lazy content
    await page.evaluate(async () => {
      const maxScroll = document.body ? document.body.scrollHeight : 0;
      const step = Math.max(650, Math.floor(window.innerHeight * 1.2));
      for (let y = 0; y < Math.min(maxScroll, 4500); y += step) {
        window.scrollTo(0, y);
        await new Promise((r) => setTimeout(r, 45));
      }
      window.scrollTo(0, 0);
    });
    await page.waitForTimeout(120);

    const title = clean(await page.title());
    const meta = await extractMeta(page);

    const bodyTextRaw = await page.evaluate(() => (document.body ? document.body.innerText || "" : ""));
    const bodyTextCleaned = clean(bodyTextRaw);

    const bodyNoShell = removeShellFromBody(bodyTextRaw, shellSig);
    const bodyNoShellCleaned = clean(bodyNoShell);

    const rawWords = countWordsExact(bodyTextCleaned);
    const cleanWords = countWordsExact(bodyNoShellCleaned);

    const usedFallback = rawWords > 0 && cleanWords < Math.floor(rawWords * 0.4);
    const effectiveBody = usedFallback ? bodyTextCleaned : bodyNoShellCleaned;

    // OCR
    const ocrTexts = await ocrAllImages(page, stats, ocrCache);
    const ocrClean = clean(ocrTexts.join("\n\n"));

    // cap & compose
    const cappedBody = capText(effectiveBody, Math.floor(MAX_CONTENT_CHARS * 0.75));
    const cappedOcr = capText(ocrClean, Math.floor(MAX_CONTENT_CHARS * 0.25));

    const words = countWordsExact(cappedBody) + countWordsExact(cappedOcr);
    const elapsed = Date.now() - startTime;

    console.log(
      `[PAGE] ✓ words=${words} rawWords=${rawWords} shellCleanWords=${cleanWords} fallbackRaw=${usedFallback} ocr_imgs=${ocrTexts.length} ocr_chars=${ocrClean.length} ${elapsed}ms`,
    );

    const links = await collectAllLinks(page, base);

    if (words < MIN_WORDS) {
      console.log(`[DROP] url=${url} reason=MIN_WORDS words=${words} MIN_WORDS=${MIN_WORDS} links_found=${links.length}`);
      return { links, page: null };
    }

    const content = `
=== META_START ===
title: ${title}
description: ${meta.desc || ""}
ogTitle: ${meta.ogTitle || ""}
canonical: ${meta.canonical || ""}
lang: ${meta.lang || ""}
headings:
- ${(meta.headings || []).join("\n- ")}
=== META_END ===

=== MAIN_CONTENT_START ===
${cappedBody}
=== MAIN_CONTENT_END ===

=== OCR_CONTENT_START ===
${cappedOcr}
=== OCR_CONTENT_END ===
`.trim();

    return {
      links,
      page: { url, title, meta, content, wordCount: words, status: "ok" },
    };
  } catch (e) {
    console.error("[PAGE ERROR]", url, e?.message || e);
    stats.errors++;
    return { links: [], page: null };
  }
}

// ================= CRAWL =================
async function crawlSmart(startUrl, siteId, job) {
  const deadline = Date.now() + MAX_SECONDS * 1000;
  console.log("\n[CRAWL START]", startUrl);
  console.log(`[CONFIG] tabs=${PARALLEL_TABS} max_pages=${MAX_PAGES} max_queue=${MAX_QUEUE} MIN_WORDS=${MIN_WORDS}`);
  if (siteId) console.log(`[SITE ID] ${siteId}`);

  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--disable-software-rasterizer"],
  });

  const stats = job.stats;
  const queue = [];
  let base = "";
  let siteShell = { header: "", footer: "" };
  let shellSig = new Set();

  // write stream (NDJSON)
  const outPath = jobFilePath(job.job_id);
  const out = fs.createWriteStream(outPath, { flags: "w" });
  job.outputPath = outPath;

  try {
    const initContext = await browser.newContext({
      viewport: { width: 1600, height: 900 },
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    });

    const initPage = await initContext.newPage();
    await initPage.route("**/*", (route) => {
      const req = route.request();
      const type = req.resourceType();
      const url = req.url();
      if (shouldBlockRequest(url, type)) return route.abort();
      return route.continue();
    });

    await initPage.goto(startUrl, { timeout: 15000, waitUntil: "domcontentloaded" });
    base = new URL(initPage.url()).origin;

    const shell = await getHeaderFooterText(initPage);
    siteShell = { header: clean(shell.headerText), footer: clean(shell.footerText) };
    shellSig = buildShellSignature(siteShell.header, siteShell.footer);
    console.log(`[SHELL] header=${siteShell.header.length} footer=${siteShell.footer.length} sig=${shellSig.size}`);

    const seedUrl = normalizeUrl(initPage.url());
    pushQueue(queue, seedUrl);

    const initialLinks = await collectAllLinks(initPage, base);
    console.log(`[CRAWL] seed=${seedUrl} initial_links=${initialLinks.length}`);

    for (const l of initialLinks) {
      const nl = normalizeUrl(l);
      if (!job.visited.has(nl) && !SKIP_URL_RE.test(nl) && !queue.includes(nl)) pushQueue(queue, nl);
    }

    await initPage.close();
    await initContext.close();

    job.baseOrigin = base;
    job.siteShell = siteShell;

    // write shell first as a special record
    out.write(JSON.stringify({ type: "shell", siteShell, baseOrigin: base }) + "\n");

    const sharedContext = await browser.newContext({
      viewport: { width: 1600, height: 900 },
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    });

    const createWorker = async (idx) => {
      const pg = await sharedContext.newPage();
      await pg.route("**/*", (route) => {
        const req = route.request();
        const type = req.resourceType();
        const url = req.url();
        if (shouldBlockRequest(url, type)) return route.abort();
        return route.continue();
      });

      while (Date.now() < deadline) {
        if (stats.saved >= MAX_PAGES) break;

        let url = null;
        while (queue.length > 0) {
          const candidate = queue.shift();
          const normalized = normalizeUrl(candidate);
          if (!job.visited.has(normalized) && !SKIP_URL_RE.test(normalized)) {
            job.visited.add(normalized);
            url = normalized;
            break;
          }
        }

        if (!url) {
          await new Promise((r) => setTimeout(r, 50));
          if (queue.length === 0) break;
          continue;
        }

        stats.visited++;
        job.lastUrl = url;

        const result = await processPage(pg, url, base, shellSig, stats, job.ocrCache);

        if (result.page) {
          stats.saved++;
          out.write(JSON.stringify({ type: "page", ...result.page }) + "\n");
        }

        for (const l of result.links || []) {
          const nl = normalizeUrl(l);
          if (!job.visited.has(nl) && !SKIP_URL_RE.test(nl) && !queue.includes(nl)) pushQueue(queue, nl);
          if (queue.length >= MAX_QUEUE) break;
        }

        if (stats.visited % 10 === 0) {
          console.log(
            `[PROGRESS] visited=${stats.visited} saved=${stats.saved} queue=${queue.length} worker=${idx} ocr_el=${stats.ocrElementsProcessed} ocr_chars=${stats.ocrCharsExtracted} errors=${stats.errors}`,
          );
        }
      }

      await pg.close();
    };

    await Promise.all(Array(PARALLEL_TABS).fill(0).map((_, i) => createWorker(i)));
    await sharedContext.close();
  } finally {
    out.end();
    await browser.close();
    console.log(
      `\n[CRAWL DONE] saved=${stats.saved} visited=${stats.visited} ocr_el=${stats.ocrElementsProcessed} ocr_chars=${stats.ocrCharsExtracted} errors=${stats.errors}`,
    );
  }

  return true;
}

// ================= JOBS =================
function cleanupJobs() {
  const now = Date.now();
  for (const [jobId, job] of jobs.entries()) {
    if (now - job.createdAt > JOB_TTL_MS) {
      try {
        if (job.outputPath && fs.existsSync(job.outputPath)) fs.unlinkSync(job.outputPath);
      } catch {}
      jobs.delete(jobId);
    }
  }
}

function startJob({ url, site_id }) {
  cleanupJobs();

  const job_id = crypto.randomUUID();
  const job = {
    job_id,
    status: "queued",
    createdAt: Date.now(),
    startedAt: null,
    finishedAt: null,
    url,
    site_id,
    error: null,
    visited: new Set(),
    ocrCache: new Map(),
    stats: { visited: 0, saved: 0, ocrElementsProcessed: 0, ocrCharsExtracted: 0, errors: 0 },
    baseOrigin: null,
    siteShell: null,
    outputPath: null,
    lastUrl: null,
  };

  jobs.set(job_id, job);
  console.log(`[JOB] created job_id=${job_id} url=${url} site_id=${site_id || "null"}`);

  (async () => {
    job.status = "processing";
    job.startedAt = Date.now();
    console.log(`[JOB] start job_id=${job_id}`);

    try {
      await crawlSmart(url, site_id || null, job);
      job.status = "ready";
      job.finishedAt = Date.now();
      console.log(`[JOB] done job_id=${job_id} status=ready saved=${job.stats.saved} visited=${job.stats.visited}`);
    } catch (e) {
      job.status = "failed";
      job.error = e instanceof Error ? e.message : String(e);
      job.finishedAt = Date.now();
      console.log(`[JOB] done job_id=${job_id} status=failed error=${job.error}`);
    }
  })();

  return job_id;
}

// ================= SERVER =================
process.on("uncaughtException", (err) => console.error("[FATAL] uncaughtException:", err));
process.on("unhandledRejection", (err) => console.error("[FATAL] unhandledRejection:", err));

http
  .createServer(async (req, res) => {
    const reqId = getReqId(req);
    const pathName = getPath(req).replace(/\/+$/, "");
    const q = getQuery(req);

    if (req.method === "OPTIONS") {
      res.writeHead(204, corsHeaders());
      return res.end();
    }

    // ✅ request log always
    const ua = typeof req.headers["user-agent"] === "string" ? req.headers["user-agent"] : "";
    console.log(`[REQ] ${reqId} ${req.method} ${pathName} ua="${ua.slice(0, 80)}"`);

    // ✅ no-auth ping for debugging render routing
    if (req.method === "GET" && pathName === "/ping") {
      res.writeHead(200, { ...corsHeaders(), "Content-Type": "text/plain; charset=utf-8" });
      return res.end("ok");
    }

    // ✅ health endpoint (set Render Health Check Path to /health)
    if (req.method === "GET" && pathName === "/health") return json(res, 200, { ok: true, port: PORT, jobs: jobs.size });

    if (req.method === "GET" && (pathName === "/" || pathName === "/status")) {
      return json(res, 200, { ok: true, jobs: jobs.size, hint: "POST /crawl then GET /result?job_id=..." });
    }

    if (req.method === "GET" && pathName === "/download") {
      if (!checkAuth(req, reqId)) return json(res, 401, { ok: false, error: "Unauthorized" });
      const job_id = q.get("job_id") || "";
      const job = jobs.get(job_id);
      if (!job || !job.outputPath || !fs.existsSync(job.outputPath)) {
        return json(res, 404, { ok: false, error: "File not found" });
      }
      res.writeHead(200, {
        ...corsHeaders(),
        "Content-Type": "application/x-ndjson; charset=utf-8",
        "Content-Disposition": `attachment; filename="crawl_${job_id}.ndjson"`,
      });
      fs.createReadStream(job.outputPath).pipe(res);
      return;
    }

    if (req.method === "GET" && pathName === "/result") {
      if (!checkAuth(req, reqId)) return json(res, 401, { ok: false, error: "Unauthorized" });

      const job_id = q.get("job_id") || "";
      const job = jobs.get(job_id);
      if (!job) return json(res, 404, { ok: false, error: "Job not found" });

      if (job.status === "queued" || job.status === "processing") {
        return json(res, 202, {
          ok: true,
          status: job.status,
          job_id,
          url: job.url,
          site_id: job.site_id,
          stats: job.stats,
          lastUrl: job.lastUrl,
        });
      }

      if (job.status === "failed") {
        return json(res, 200, { ok: false, status: "failed", job_id, error: job.error, stats: job.stats });
      }

      return json(res, 200, {
        ok: true,
        status: "ready",
        job_id,
        stats: job.stats,
        baseOrigin: job.baseOrigin,
        pages_url: `/download?job_id=${encodeURIComponent(job_id)}`,
      });
    }

        // ✅ SYNC endpoint: returns content directly in one request
    if (req.method === "POST" && pathName === "/crawl-sync") {
      if (!checkAuth(req, reqId)) return json(res, 401, { ok: false, error: "Unauthorized" });

      let payload = {};
      try {
        const body = await readBody(req);
        payload = JSON.parse(body || "{}");
      } catch {
        return json(res, 400, { ok: false, error: "Invalid JSON" });
      }

      const url = typeof payload.url === "string" ? payload.url.trim() : "";
      const site_id =
        (typeof payload.sessionId === "string" && payload.sessionId.trim()) ||
        (typeof payload.site_id === "string" && payload.site_id.trim()) ||
        "";

      if (!url) return json(res, 400, { ok: false, error: "Missing url" });

      // run a single-job crawl and return NDJSON as a string (fast + low memory)
      const job = {
        job_id: crypto.randomUUID(),
        status: "processing",
        createdAt: Date.now(),
        startedAt: Date.now(),
        finishedAt: null,
        url,
        site_id,
        error: null,
        visited: new Set(),
        ocrCache: new Map(),
        stats: { visited: 0, saved: 0, ocrElementsProcessed: 0, ocrCharsExtracted: 0, errors: 0 },
        baseOrigin: null,
        siteShell: null,
        outputPath: null,
        lastUrl: null,
      };

      try {
        await crawlSmart(url, site_id || null, job);

        // read produced ndjson file and return it (bounded by MAX_PAGES/MAX_CONTENT_CHARS)
        if (job.outputPath && fs.existsSync(job.outputPath)) {
          const ndjson = fs.readFileSync(job.outputPath, "utf-8");
          return json(res, 200, {
            ok: true,
            status: "ready",
            job_id: job.job_id,
            stats: job.stats,
            baseOrigin: job.baseOrigin,
            siteShell: job.siteShell,
            ndjson,
          });
        }

        return json(res, 200, { ok: true, status: "ready", job_id: job.job_id, stats: job.stats, ndjson: "" });
      } catch (e) {
        const err = e instanceof Error ? e.message : String(e);
        return json(res, 500, { ok: false, status: "failed", error: err });
      }
    }

      console.log(`[CRAWL] accepted reqId=${reqId} url=${url} site_id=${site_id || "null"}`);
      const job_id = startJob({ url, site_id });
      return json(res, 202, { ok: true, accepted: true, job_id, status: "queued", url, site_id });

    return json(res, 404, { ok: false, error: "Not found" });
  })
  .listen(PORT, "0.0.0.0", () => console.log(`[BOOT] crawler listening on 0.0.0.0:${PORT}`));

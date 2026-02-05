import http from "http";
import { chromium } from "playwright";
import crypto from "crypto";

const PORT = Number(process.env.PORT || 10000);

// Security
const CRAWLER_SECRET = process.env.CRAWLER_SECRET || "";
const CRAWLER_TOKEN = process.env.CRAWLER_TOKEN || "";

// OCR (optional)
const GOOGLE_VISION_API_KEY = process.env.GOOGLE_VISION_API_KEY || "";

// ================= LIMITS =================
const MAX_SECONDS = Number(process.env.MAX_SECONDS || 180);
const MIN_WORDS = Number(process.env.MIN_WORDS || 20);

const PARALLEL_TABS = Number(process.env.PARALLEL_TABS || 2);
const PARALLEL_OCR = Number(process.env.PARALLEL_OCR || 6);
const OCR_TIMEOUT_MS = Number(process.env.OCR_TIMEOUT_MS || 6000);

const MAX_PAGES = Number(process.env.MAX_PAGES || 120);
const MAX_QUEUE = Number(process.env.MAX_QUEUE || 1200);
const MAX_CONTENT_CHARS = Number(process.env.MAX_CONTENT_CHARS || 70000);
const MAX_OCR_CACHE = Number(process.env.MAX_OCR_CACHE || 400);

// In-memory job store
const JOB_TTL_MS = 15 * 60 * 1000;
const jobs = new Map();
const visited = new Set();
const globalOcrCache = new Map();

const SKIP_URL_RE =
  /(wp-content\/uploads|media|gallery|video|photo|attachment|privacy|terms|cookies|gdpr)/i;

const SKIP_OCR_RE = /\/(logo|favicon|spinner|avatar|pixel|spacer|blank|transparent)\.|\/icons?\//i;

console.log(`[BOOT] neo-browser-crawler starting... PORT=${PORT}`);
setInterval(() => {
  const m = process.memoryUsage();
  console.log(
    `[MEM] rss=${Math.round(m.rss / 1024 / 1024)}MB heapUsed=${Math.round(m.heapUsed / 1024 / 1024)}MB`,
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
    const u = new URL(req.url, "http://localhost");
    return u.pathname;
  } catch {
    return "/";
  }
}

function getQuery(req) {
  try {
    const u = new URL(req.url, "http://localhost");
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

  if (!ok) console.log(`[AUTH] ${reqId} Unauthorized`);
  return ok;
}

// ================= OCR =================
async function ocrImageUrl(imageUrl) {
  if (!GOOGLE_VISION_API_KEY) return "";

  try {
    if (globalOcrCache.has(imageUrl)) return globalOcrCache.get(imageUrl);

    const body = {
      requests: [
        {
          image: { source: { imageUri: imageUrl } },
          features: [{ type: "TEXT_DETECTION" }],
        },
      ],
    };

    const r = await fetch(
      `https://vision.googleapis.com/v1/images:annotate?key=${GOOGLE_VISION_API_KEY}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      },
    );

    const data = await r.json().catch(() => null);
    const text =
      data?.responses?.[0]?.fullTextAnnotation?.text ||
      data?.responses?.[0]?.textAnnotations?.[0]?.description ||
      "";

    if (globalOcrCache.size >= MAX_OCR_CACHE) globalOcrCache.clear();
    globalOcrCache.set(imageUrl, text || "");
    return text || "";
  } catch {
    return "";
  }
}

async function ocrAllImages(page, stats) {
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
      if (img.w < 80 || img.h < 60) return false;
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
            ocrImageUrl(img.src),
            new Promise((resolve) => setTimeout(() => resolve(""), OCR_TIMEOUT_MS)),
          ]);
          const text = await p;
          if (text && String(text).trim().length > 0) {
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

// normalize text into lines, remove empty/noisy short lines
function normalizeLines(text) {
  return clean(text)
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l.length >= 4);
}

// Build a signature set from header/footer lines (to remove later)
function buildShellSignature(headerText, footerText) {
  const lines = [...normalizeLines(headerText), ...normalizeLines(footerText)];

  // keep only the strongest "repeating" candidates (not too long, not pure numbers)
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

// Remove lines that exist in shell signature
function removeShellFromBody(bodyText, shellSig) {
  const bodyLines = normalizeLines(bodyText);
  const kept = [];

  for (const l of bodyLines) {
    const key = l.replace(/\s+/g, " ").trim().toLowerCase();
    if (shellSig.has(key)) continue;
    kept.push(l);
  }

  // additional cleanup: remove duplicated consecutive lines
  const finalLines = [];
  let prev = "";
  for (const l of kept) {
    const k = l.toLowerCase();
    if (k === prev) continue;
    finalLines.push(l);
    prev = k;
  }

  return finalLines.join("\n");
}

async function getHeaderFooterText(page) {
  try {
    return await page.evaluate(() => {
      const header = document.querySelector("header");
      const footer = document.querySelector("footer");

      const headerText = header ? (header.innerText || "") : "";
      const footerText = footer ? (footer.innerText || "") : "";

      return { headerText, footerText };
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

// ================= PROCESS PAGE =================
async function processPage(page, url, base, shellSig, stats) {
  const startTime = Date.now();

  try {
    console.log("[PAGE]", url);
    await page.goto(url, { timeout: 15000, waitUntil: "domcontentloaded" });

    // quick scroll to trigger lazy content
    await page.evaluate(async () => {
      const scrollStep = Math.max(400, Math.floor(window.innerHeight * 0.9));
      const maxScroll = document.body.scrollHeight;
      for (let pos = 0; pos < maxScroll; pos += scrollStep) {
        window.scrollTo(0, pos);
        await new Promise((r) => setTimeout(r, 60));
      }
      window.scrollTo(0, maxScroll);
    });
    await page.waitForTimeout(250);

    const title = clean(await page.title());

    const bodyText = await page.evaluate(() => (document.body ? document.body.innerText || "" : ""));
    const bodyClean = removeShellFromBody(bodyText, shellSig);

    const ocrTexts = await ocrAllImages(page, stats);
    const ocrClean = clean(ocrTexts.join("\n\n"));

    // cap memory
    const cappedBody = capText(bodyClean, Math.floor(MAX_CONTENT_CHARS * 0.75));
    const cappedOcr = capText(ocrClean, Math.floor(MAX_CONTENT_CHARS * 0.25));

    const content = `
=== MAIN_CONTENT_START ===
${cappedBody}
=== MAIN_CONTENT_END ===

=== OCR_CONTENT_START ===
${cappedOcr}
=== OCR_CONTENT_END ===
`.trim();

    const words = countWordsExact(cappedBody) + countWordsExact(cappedOcr);
    const elapsed = Date.now() - startTime;

    console.log(`[PAGE] ✓ words=${words} ocr_imgs=${ocrTexts.length} ${elapsed}ms`);

    const links = await collectAllLinks(page, base);

    if (words < MIN_WORDS) {
      return { links, page: null };
    }

    return {
      links,
      page: {
        url,
        title,
        content,
        wordCount: words,
        status: "ok",
      },
    };
  } catch (e) {
    console.error("[PAGE ERROR]", url, e?.message || e);
    stats.errors++;
    return { links: [], page: null };
  }
}

// ================= CRAWL (NO SITEMAP) =================
async function crawlSmart(startUrl, siteId = null) {
  const deadline = Date.now() + MAX_SECONDS * 1000;
  console.log("\n[CRAWL START]", startUrl);
  console.log(`[CONFIG] tabs=${PARALLEL_TABS} max_pages=${MAX_PAGES} max_queue=${MAX_QUEUE}`);
  if (siteId) console.log(`[SITE ID] ${siteId}`);

  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--disable-software-rasterizer"],
  });

  const stats = {
    visited: 0,
    saved: 0,
    ocrElementsProcessed: 0,
    ocrCharsExtracted: 0,
    errors: 0,
  };

  const pages = [];
  const queue = [];
  let base = "";

  let siteShell = { header: "", footer: "" };
  let shellSig = new Set();

  try {
    const initContext = await browser.newContext({
      viewport: { width: 1600, height: 900 },
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    });
    const initPage = await initContext.newPage();

    await initPage.goto(startUrl, { timeout: 15000, waitUntil: "domcontentloaded" });
    base = new URL(initPage.url()).origin;

    // capture header/footer ONCE
    const shell = await getHeaderFooterText(initPage);
    siteShell = {
      header: clean(shell.headerText),
      footer: clean(shell.footerText),
    };
    shellSig = buildShellSignature(siteShell.header, siteShell.footer);

    console.log(`[SHELL] header=${siteShell.header.length} chars footer=${siteShell.footer.length} chars sig=${shellSig.size} lines`);

    // seed queue from the first page only
    pushQueue(queue, normalizeUrl(initPage.url()));
    const initialLinks = await collectAllLinks(initPage, base);
    for (const l of initialLinks) {
      const nl = normalizeUrl(l);
      if (!visited.has(nl) && !SKIP_URL_RE.test(nl) && !queue.includes(nl)) pushQueue(queue, nl);
    }

    await initPage.close();
    await initContext.close();

    console.log(`[CRAWL] Initial queue size: ${queue.length}`);

    const sharedContext = await browser.newContext({
      viewport: { width: 1600, height: 900 },
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    });

    const createWorker = async (idx) => {
      const pg = await sharedContext.newPage();

      while (Date.now() < deadline) {
        if (pages.length >= MAX_PAGES) break;

        let url = null;
        while (queue.length > 0) {
          const candidate = queue.shift();
          const normalized = normalizeUrl(candidate);
          if (!visited.has(normalized) && !SKIP_URL_RE.test(normalized)) {
            visited.add(normalized);
            url = normalized;
            break;
          }
        }

        if (!url) {
          await new Promise((r) => setTimeout(r, 40));
          if (queue.length === 0) break;
          continue;
        }

        stats.visited++;
        const result = await processPage(pg, url, base, shellSig, stats);

        if (result.page) {
          pages.push(result.page);
          stats.saved++;
        }

        for (const l of result.links || []) {
          const nl = normalizeUrl(l);
          if (!visited.has(nl) && !SKIP_URL_RE.test(nl) && !queue.includes(nl)) pushQueue(queue, nl);
          if (queue.length >= MAX_QUEUE) break;
        }

        if (stats.visited % 10 === 0) {
          console.log(`[PROGRESS] visited=${stats.visited} saved=${stats.saved} queue=${queue.length} pages_in_mem=${pages.length} worker=${idx}`);
        }
      }

      await pg.close();
    };

    await Promise.all(Array(PARALLEL_TABS).fill(0).map((_, i) => createWorker(i)));
    await sharedContext.close();
  } finally {
    await browser.close();
    console.log(`\n[CRAWL DONE] saved=${stats.saved} visited=${stats.visited} queue_left=${queue.length}`);
  }

  return { siteShell, pages, stats };
}

// ================= JOBS =================
function cleanupJobs() {
  const now = Date.now();
  for (const [jobId, job] of jobs.entries()) {
    if (now - job.createdAt > JOB_TTL_MS) jobs.delete(jobId);
  }
}

function startJob({ url, site_id }) {
  const job_id = crypto.randomUUID();
  const job = {
    job_id,
    status: "queued",
    createdAt: Date.now(),
    startedAt: null,
    finishedAt: null,
    url,
    site_id,
    result: null,
    error: null,
  };
  jobs.set(job_id, job);

  (async () => {
    cleanupJobs();
    globalOcrCache.clear();
    visited.clear();

    job.status = "processing";
    job.startedAt = Date.now();

    try {
      const result = await crawlSmart(url, site_id || null);
      job.status = "ready";
      job.result = result;
      job.finishedAt = Date.now();
    } catch (e) {
      job.status = "failed";
      job.error = e instanceof Error ? e.message : String(e);
      job.finishedAt = Date.now();
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
    const path = getPath(req);
    const q = getQuery(req);

    if (req.method === "OPTIONS") {
      res.writeHead(204, corsHeaders());
      return res.end();
    }

    console.log(`[REQ] ${reqId} ${req.method} ${path}`);

    if (req.method === "GET" && path === "/health") return json(res, 200, { ok: true });

    if (req.method === "GET" && (path === "/" || path === "/status")) {
      return json(res, 200, { ok: true, jobs: jobs.size });
    }

    if (req.method === "GET" && path === "/result") {
      if (!checkAuth(req, reqId)) return json(res, 401, { ok: false, error: "Unauthorized" });

      const job_id = q.get("job_id") || "";
      const job = jobs.get(job_id);
      if (!job) return json(res, 404, { ok: false, error: "Job not found" });

      if (job.status === "queued" || job.status === "processing") {
        return json(res, 202, { ok: true, status: job.status, job_id, url: job.url, site_id: job.site_id });
      }

      if (job.status === "failed") {
        return json(res, 200, { ok: false, status: "failed", job_id, error: job.error });
      }

      return json(res, 200, { ok: true, status: "ready", job_id, result: job.result });
    }

    if (req.method === "POST" && path === "/crawl") {
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

      const sessionToken = typeof payload.sessionToken === "string" ? payload.sessionToken.trim() : "";
      if (CRAWLER_TOKEN && sessionToken && sessionToken !== CRAWLER_TOKEN) {
        console.log(`[AUTH] ${reqId} sessionToken mismatch`);
        return json(res, 401, { ok: false, error: "Unauthorized" });
      }

      if (!url) return json(res, 400, { ok: false, error: "Missing url" });

      const job_id = startJob({ url, site_id });
      return json(res, 202, { ok: true, accepted: true, job_id, status: "queued", url, site_id });
    }

    return json(res, 404, { ok: false, error: "Not found" });
  })
  .listen(PORT, () => console.log(`[BOOT] neo-browser-crawler listening on :${PORT}`));

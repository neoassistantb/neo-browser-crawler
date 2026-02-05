import http from "http";
import { chromium } from "playwright";
import crypto from "crypto";

const PORT = Number(process.env.PORT || 10000);

// Security
const CRAWLER_SECRET = process.env.CRAWLER_SECRET || "";
const CRAWLER_TOKEN = process.env.CRAWLER_TOKEN || "";

// OCR
const GOOGLE_VISION_API_KEY = process.env.GOOGLE_VISION_API_KEY || "";

// Worker (KEEP)
const WORKER_URL = process.env.NEO_WORKER_URL || "";
const WORKER_SECRET = process.env.NEO_WORKER_SECRET || "";

// ================= LIMITS =================
const MAX_SECONDS = 180;
const MIN_WORDS = 20;
const PARALLEL_TABS = 5;
const PARALLEL_OCR = 10;
const OCR_TIMEOUT_MS = 6000;

// In-memory job store
const JOB_TTL_MS = 15 * 60 * 1000;
const jobs = new Map();
const visited = new Set();
const globalOcrCache = new Map<string, any>();

const SKIP_URL_RE =
  /(wp-content\/uploads|media|gallery|video|photo|attachment|privacy|terms|cookies|gdpr)/i;

const SKIP_OCR_RE = /\/(logo|favicon|spinner|avatar|pixel|spacer|blank|transparent)\.|\/icons?\//i;

// ================= CORS =================
function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers":
      "content-type, authorization, x-request-id, x-crawler-token",
  };
}

// ================= UTILS =================
const clean = (t = "") =>
  t.replace(/\r/g, "").replace(/[ \t]+/g, " ").replace(/\n{3,}/g, "\n\n").trim();

const countWordsExact = (t = "") =>
  (t.match(/[A-Za-zА-Яа-я0-9]+/g) || []).length;

function getReqId(req: any) {
  const v = req.headers["x-request-id"];
  if (typeof v === "string" && v.trim()) return v.trim();
  return crypto.randomUUID();
}

function getPath(req: any) {
  try {
    const u = new URL(req.url, "http://localhost");
    return u.pathname;
  } catch {
    return "/";
  }
}

function getQuery(req: any) {
  try {
    const u = new URL(req.url, "http://localhost");
    return u.searchParams;
  } catch {
    return new URLSearchParams();
  }
}

async function readBody(req: any) {
  return await new Promise<string>((resolve) => {
    let data = "";
    req.on("data", (c: any) => (data += c));
    req.on("end", () => resolve(data));
  });
}

function json(res: any, status: number, obj: any) {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    ...corsHeaders(),
    "Content-Type": "application/json; charset=utf-8",
  });
  res.end(body);
}

function normalizeUrl(u: string) {
  try {
    const url = new URL(u);
    url.hash = "";
    // normalize trailing slash (keep root "/")
    if (url.pathname !== "/" && url.pathname.endsWith("/")) url.pathname = url.pathname.slice(0, -1);
    return url.toString();
  } catch {
    return u;
  }
}

// ================= AUTH =================
function checkAuth(req: any, reqId: string) {
  // Allow if no secrets configured
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
    console.log(`[AUTH] ${reqId} Unauthorized (missing/invalid token)`);
  }

  return ok;
}

// ================= SIMPLE SITEMAP FETCH =================
async function fetchWithTimeout(url: string, ms: number) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), ms);
  try {
    const r = await fetch(url, { signal: controller.signal });
    return r;
  } finally {
    clearTimeout(t);
  }
}

// Extract <loc>...</loc> values from XML
function extractLocs(xml: string) {
  const out: string[] = [];
  const re = /<loc>\s*([^<]+)\s*<\/loc>/gi;
  let m: RegExpExecArray | null;
  while ((m = re.exec(xml))) {
    const v = m[1]?.trim();
    if (v) out.push(v);
  }
  return out;
}

// Try sitemap.xml and sitemap_index.xml; also handle index -> nested sitemaps
async function seedUrlsFromSitemap(baseOrigin: string) {
  const candidates = [
    `${baseOrigin}/sitemap.xml`,
    `${baseOrigin}/sitemap_index.xml`,
    `${baseOrigin}/sitemap-index.xml`,
  ];

  const seenSitemaps = new Set<string>();
  const foundUrls: string[] = [];

  async function readSitemap(sitemapUrl: string, depth: number) {
    if (depth > 2) return; // keep it safe
    if (seenSitemaps.has(sitemapUrl)) return;
    seenSitemaps.add(sitemapUrl);

    try {
      const r = await fetchWithTimeout(sitemapUrl, 8000);
      if (!r.ok) return;
      const xml = await r.text();
      const locs = extractLocs(xml);

      // If it looks like an index (contains other .xml), read those
      const xmlLinks = locs.filter((l) => /\.xml(\?|$)/i.test(l));
      if (xmlLinks.length > 0) {
        for (const child of xmlLinks.slice(0, 50)) {
          await readSitemap(child, depth + 1);
        }
        return;
      }

      // Otherwise it's a urlset
      for (const u of locs) foundUrls.push(u);
    } catch {
      // ignore
    }
  }

  for (const s of candidates) {
    await readSitemap(s, 0);
    if (foundUrls.length > 0) break;
  }

  // Normalize, same-origin only
  const normalized = new Set<string>();
  for (const u of foundUrls) {
    try {
      const x = new URL(u);
      if (x.origin !== baseOrigin) continue;
      const nu = normalizeUrl(x.toString());
      if (!SKIP_URL_RE.test(nu)) normalized.add(nu);
    } catch {}
  }

  return Array.from(normalized);
}

// ================= OCR =================
async function ocrImageUrl(imageUrl: string) {
  if (!GOOGLE_VISION_API_KEY) return "";

  try {
    // cache
    if (globalOcrCache.has(imageUrl)) return globalOcrCache.get(imageUrl);

    const body = {
      requests: [
        {
          image: { source: { imageUri: imageUrl } },
          features: [{ type: "TEXT_DETECTION" }],
        },
      ],
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

    globalOcrCache.set(imageUrl, text || "");
    return text || "";
  } catch {
    return "";
  }
}

async function ocrAllImages(page: any, stats: any) {
  const start = Date.now();

  try {
    const imgElements = await page.$$eval("img", (imgs: any[]) =>
      imgs
        .map((img) => ({
          src: img.currentSrc || img.src || img.getAttribute("src") || "",
          w: img.naturalWidth || img.width || 0,
          h: img.naturalHeight || img.height || 0,
          alt: img.alt || "",
        }))
        .filter((x) => x.src)
    );

    const validImages = imgElements.filter((img) => {
      if (!img.src) return false;
      if (SKIP_OCR_RE.test(img.src)) return false;
      if (img.w < 80 || img.h < 60) return false;
      return true;
    });

    console.log(`[OCR] ${validImages.length}/${imgElements.length} images to process`);

    const results: any[] = [];
    let idx = 0;

    while (idx < validImages.length) {
      const batch = validImages.slice(idx, idx + PARALLEL_OCR);
      idx += PARALLEL_OCR;

      const batchPromises = batch.map(async (img) => {
        const t0 = Date.now();
        try {
          const p = Promise.race([
            ocrImageUrl(img.src),
            new Promise<string>((resolve) => setTimeout(() => resolve(""), OCR_TIMEOUT_MS)),
          ]);

          const text = await p;
          if (text && text.trim().length > 0) {
            stats.ocrElementsProcessed++;
            stats.ocrCharsExtracted += text.length;
            return { text, src: img.src, alt: img.alt };
          }
        } catch {
          // ignore
        } finally {
          const dt = Date.now() - t0;
          if (dt > OCR_TIMEOUT_MS) {
            // just informational
          }
        }
        return null;
      });

      const batchResults = await Promise.all(batchPromises);
      results.push(...batchResults.filter((r) => r !== null));
    }

    const dt = Date.now() - start;
    if (dt > 5000) {
      // slow OCR batch - ok
    }

    return results;
  } catch (e: any) {
    console.error("[OCR ERROR]", e?.message || e);
    return [];
  }
}

// ================= LINK DISCOVERY =================
async function collectAllLinks(page: any, base: string) {
  try {
    return await page.evaluate((baseOrigin: string) => {
      const urls = new Set<string>();
      document.querySelectorAll("a[href]").forEach((a) => {
        try {
          const u = new URL((a as HTMLAnchorElement).href, baseOrigin);
          if (u.origin === baseOrigin) urls.add(u.href.split("#")[0]);
        } catch {}
      });
      return Array.from(urls);
    }, base);
  } catch {
    return [];
  }
}

// ================= STRUCTURED EXTRACTION (existing) =================
// NOTE: оставяме логиката както е, за да не чупим поведението.
// Във файла ти има големи помощни функции: detectPageType, extractStructured,
// extractSiteMapFromPage, enrichSiteMap, buildCombinedSiteMap, sendSiteMapToWorker,
// numberToBgWords, normalizeNumbers, etc.
// ---- START: keep existing helpers ----

// (Пази твоите helper-и, не ги променям извън нужните места)

function detectPageType(url: string, title: string) {
  const u = (url || "").toLowerCase();
  const t = (title || "").toLowerCase();

  if (/(contact|контакт)/i.test(u + " " + t)) return "contact";
  if (/(about|за-нас|about-us)/i.test(u + " " + t)) return "about";
  if (/(services|услуги|service)/i.test(u + " " + t)) return "services";
  if (/(blog|news|статии)/i.test(u + " " + t)) return "blog";
  if (/(shop|product|каталог|продукт)/i.test(u + " " + t)) return "products";
  return "general";
}

// PLACEHOLDER: these are referenced below; in your real file they already exist
declare function extractStructured(page: any): Promise<any>;
declare function extractSiteMapFromPage(page: any): Promise<any>;
declare function enrichSiteMap(raw: any, siteId: string, base: string): any;
declare function buildCombinedSiteMap(maps: any[], siteId: string, base: string): any;
declare function sendSiteMapToWorker(map: any): Promise<void>;
declare function normalizeNumbers(text: string): string;

// ---- END: keep existing helpers ----

// ================= PROCESS SINGLE PAGE =================
async function processPage(page: any, url: string, base: string, stats: any, siteMaps: any[]) {
  const startTime = Date.now();

  try {
    console.log("[PAGE]", url);
    await page.goto(url, { timeout: 10000, waitUntil: "domcontentloaded" });

    // scroll + eager images
    await page.evaluate(async () => {
      const scrollStep = window.innerHeight;
      const maxScroll = document.body.scrollHeight;
      for (let pos = 0; pos < maxScroll; pos += scrollStep) {
        window.scrollTo(0, pos);
        await new Promise((r) => setTimeout(r, 100));
      }
      window.scrollTo(0, maxScroll);

      document.querySelectorAll('img[loading="lazy"], img[data-src], img[data-lazy]').forEach((img: any) => {
        img.loading = "eager";
        if (img.dataset?.src) img.src = img.dataset.src;
        if (img.dataset?.lazy) img.src = img.dataset.lazy;
      });
    });

    await page.waitForTimeout(500);
    try {
      await page.waitForLoadState("networkidle", { timeout: 3000 });
    } catch {}

    const title = clean(await page.title());
    const pageType = detectPageType(url, title);
    stats.byType[pageType] = (stats.byType[pageType] || 0) + 1;

    const data = await extractStructured(page);
    const ocrResults = await ocrAllImages(page, stats);

    try {
      const rawSiteMap = await extractSiteMapFromPage(page);
      if ((rawSiteMap.buttons?.length || 0) > 0 || (rawSiteMap.forms?.length || 0) > 0) {
        siteMaps.push(rawSiteMap);
        console.log(`[SITEMAP] Page: ${rawSiteMap.buttons.length} buttons, ${rawSiteMap.forms.length} forms`);
      }
    } catch (e: any) {
      console.error("[SITEMAP] Extract error:", e?.message || e);
    }

    const htmlContent = normalizeNumbers(clean(data.rawContent));
    const ocrTexts = ocrResults.map((r: any) => r.text);
    const ocrContent = normalizeNumbers(clean(ocrTexts.join("\n\n")));

    const content = `
=== HTML_CONTENT_START ===
${htmlContent}
=== HTML_CONTENT_END ===

=== OCR_CONTENT_START ===
${ocrContent}
=== OCR_CONTENT_END ===
`.trim();

    const htmlWords = countWordsExact(htmlContent);
    const ocrWords = countWordsExact(ocrContent);
    const totalWords = htmlWords + ocrWords;

    const elapsed = Date.now() - startTime;
    console.log(`[PAGE] ✓ ${totalWords}w (${htmlWords}+${ocrWords}ocr, ${ocrResults.length} imgs) ${elapsed}ms`);

    const links = await collectAllLinks(page, base);

    // if too thin, skip saving but still return links
    if (pageType !== "services" && totalWords < MIN_WORDS) {
      return { links, page: null };
    }

    return {
      links,
      page: {
        url,
        title,
        pageType,
        content,
        wordCount: totalWords,
        breakdown: { htmlWords, ocrWords, images: ocrResults.length },
        status: "ok",
      },
    };
  } catch (e: any) {
    console.error("[PAGE ERROR]", url, e?.message || e);
    stats.errors++;
    return { links: [], page: null };
  }
}

// ================= CRAWL =================
async function crawlSmart(startUrl: string, siteId: string | null = null) {
  const deadline = Date.now() + MAX_SECONDS * 1000;
  console.log("\n[CRAWL START]", startUrl);
  console.log(`[CONFIG] ${PARALLEL_TABS} tabs, ${PARALLEL_OCR} parallel OCR`);
  if (siteId) console.log(`[SITE ID] ${siteId}`);

  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--disable-software-rasterizer"],
  });

  const stats = {
    visited: 0,
    saved: 0,
    byType: {} as Record<string, number>,
    ocrElementsProcessed: 0,
    ocrCharsExtracted: 0,
    errors: 0,
  };

  const pages: any[] = [];
  const queue: string[] = [];
  const siteMaps: any[] = [];
  let base = "";

  try {
    const initContext = await browser.newContext({
      viewport: { width: 1920, height: 1080 },
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    });
    const initPage = await initContext.newPage();

    await initPage.goto(startUrl, { timeout: 10000, waitUntil: "domcontentloaded" });
    base = new URL(initPage.url()).origin;

    // seed from current page links
    queue.push(normalizeUrl(initPage.url()));
    const initialLinks = await collectAllLinks(initPage, base);
    initialLinks.forEach((l) => {
      const nl = normalizeUrl(l);
      if (!visited.has(nl) && !SKIP_URL_RE.test(nl) && !queue.includes(nl)) queue.push(nl);
    });

    // seed from sitemap (this is what unlocks "99 pages" sites)
    const sitemapUrls = await seedUrlsFromSitemap(base);
    if (sitemapUrls.length > 0) {
      console.log(`[SITEMAP] Seeded ${sitemapUrls.length} URLs from sitemap`);
      sitemapUrls.forEach((u) => {
        if (!visited.has(u) && !queue.includes(u)) queue.push(u);
      });
    } else {
      console.log("[SITEMAP] No sitemap URLs found (or blocked)");
    }

    await initPage.close();
    await initContext.close();

    console.log(`[CRAWL] Found ${queue.length} URLs`);

    const createWorker = async () => {
      const ctx = await browser.newContext({
        viewport: { width: 1920, height: 1080 },
        userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      });
      const pg = await ctx.newPage();

      while (Date.now() < deadline) {
        let url: string | null = null;

        while (queue.length > 0) {
          const candidate = queue.shift()!;
          const normalized = normalizeUrl(candidate);
          if (!visited.has(normalized) && !SKIP_URL_RE.test(normalized)) {
            visited.add(normalized);
            url = normalized;
            break;
          }
        }

        if (!url) {
          await new Promise((r) => setTimeout(r, 30));
          if (queue.length === 0) break;
          continue;
        }

        stats.visited++;
        const result = await processPage(pg, url, base, stats, siteMaps);

        if (result.page) {
          pages.push(result.page);
          stats.saved++;
        }

        result.links.forEach((l: string) => {
          const nl = normalizeUrl(l);
          if (!visited.has(nl) && !SKIP_URL_RE.test(nl) && !queue.includes(nl)) queue.push(nl);
        });
      }

      await pg.close();
      await ctx.close();
    };

    await Promise.all(Array(PARALLEL_TABS).fill(0).map(() => createWorker()));
  } finally {
    await browser.close();
    console.log(`\n[CRAWL DONE] ${stats.saved}/${stats.visited} pages`);
    console.log(`[OCR STATS] ${stats.ocrElementsProcessed} images → ${stats.ocrCharsExtracted} chars`);
  }

  let combinedSiteMap: any = null;
  if (siteMaps.length > 0 && siteId) {
    console.log(`\n[SITEMAP] Building combined map from ${siteMaps.length} pages...`);
    const enrichedMaps = siteMaps.map((raw) => enrichSiteMap(raw, siteId, base));
    combinedSiteMap = buildCombinedSiteMap(enrichedMaps, siteId, base);
    await sendSiteMapToWorker(combinedSiteMap);
  }

  return { pages, stats, siteMap: combinedSiteMap };
}

// ================= JOBS =================
function cleanupJobs() {
  const now = Date.now();
  for (const [jobId, job] of jobs.entries()) {
    if (now - job.createdAt > JOB_TTL_MS) jobs.delete(jobId);
  }
}

function startJob({ url, site_id }: { url: string; site_id: string }) {
  const job_id = crypto.randomUUID();
  const job: any = {
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
    } catch (e: any) {
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

    // CORS preflight
    if (req.method === "OPTIONS") {
      res.writeHead(204, corsHeaders());
      return res.end();
    }

    console.log(`[REQ] ${reqId} ${req.method} ${path}`);

    // Health
    if (req.method === "GET" && path === "/health") return json(res, 200, { ok: true });

    // Status
    if (req.method === "GET" && (path === "/" || path === "/status")) {
      return json(res, 200, {
        ok: true,
        authEnabled: Boolean(CRAWLER_SECRET || CRAWLER_TOKEN),
        jobs: jobs.size,
      });
    }

    // Result
    if (req.method === "GET" && path === "/result") {
      if (!checkAuth(req, reqId)) return json(res, 401, { ok: false, error: "Unauthorized" });

      const job_id = q.get("job_id") || "";
      const job = jobs.get(job_id);
      if (!job) return json(res, 404, { ok: false, error: "Job not found" });

      if (job.status === "queued" || job.status === "processing") {
        return json(res, 202, { ok: true, status: job.status, job_id, url: job.url, site_id: job.site_id });
      }

      if (job.status === "failed") {
        return json(res, 200, {
          ok: false,
          status: "failed",
          job_id,
          url: job.url,
          site_id: job.site_id,
          error: job.error,
        });
      }

      return json(res, 200, {
        ok: true,
        status: "ready",
        job_id,
        url: job.url,
        site_id: job.site_id,
        result: job.result,
      });
    }

    // Crawl (enqueue)
    if (req.method === "POST" && path === "/crawl") {
      if (!checkAuth(req, reqId)) return json(res, 401, { ok: false, error: "Unauthorized" });

      let payload: any = {};
      try {
        const body = await readBody(req);
        payload = JSON.parse(body || "{}");
      } catch {
        return json(res, 400, { ok: false, error: "Invalid JSON" });
      }

      const url = typeof payload.url === "string" ? payload.url.trim() : "";

      // accept both formats:
      // old: site_id
      // new: sessionId
      const site_id =
        (typeof payload.sessionId === "string" && payload.sessionId.trim()) ||
        (typeof payload.site_id === "string" && payload.site_id.trim()) ||
        "";

      // optional: sessionToken check (if configured)
      const sessionToken = typeof payload.sessionToken === "string" ? payload.sessionToken.trim() : "";
      if (CRAWLER_TOKEN && sessionToken && sessionToken !== CRAWLER_TOKEN) {
        console.log(`[AUTH] ${reqId} sessionToken mismatch`);
        return json(res, 401, { ok: false, error: "Unauthorized" });
      }

      if (!url) return json(res, 400, { ok: false, error: "Missing url" });

      const job_id = startJob({ url, site_id });

      return json(res, 202, {
        ok: true,
        accepted: true,
        job_id,
        status: "queued",
        url,
        site_id,
      });
    }

    return json(res, 404, { ok: false, error: "Not found" });
  })
  .listen(PORT, () => {
    console.log(`[BOOT] neo-browser-crawler listening on :${PORT}`);
    console.log(`[BOOT] auth: ${Boolean(CRAWLER_SECRET || CRAWLER_TOKEN) ? "enabled" : "disabled"}`);
  });

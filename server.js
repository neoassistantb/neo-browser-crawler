import http from "http";
import { chromium } from "playwright";
import crypto from "crypto";

const PORT = Number(process.env.PORT || 10000);

// Security
const CRAWLER_SECRET = process.env.CRAWLER_SECRET || "";

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
const jobs = new Map(); // job_id -> { status, createdAt, startedAt, finishedAt, url, site_id, result, error }

const visited = new Set();
const globalOcrCache = new Map();

const SKIP_URL_RE =
  /(wp-content\/uploads|media|gallery|video|photo|attachment|privacy|terms|cookies|gdpr)/i;

const SKIP_OCR_RE = /\/(logo|favicon|spinner|avatar|pixel|spacer|blank|transparent)\.|\/icons?\//i;

// ================= UTILS =================
const clean = (t = "") =>
  t.replace(/\r/g, "").replace(/[ \t]+/g, " ").replace(/\n{3,}/g, "\n\n").trim();

const countWordsExact = (t = "") => t.split(/\s+/).filter(Boolean).length;

function json(res, statusCode, obj) {
  res.writeHead(statusCode, { "Content-Type": "application/json" });
  res.end(JSON.stringify(obj));
}

function getReqId(req) {
  const h = req.headers["x-request-id"];
  if (typeof h === "string" && h.trim()) return h.trim();
  return crypto.randomUUID();
}

function getPath(req) {
  try {
    const url = new URL(req.url || "/", "http://localhost");
    return url.pathname || "/";
  } catch {
    return req.url || "/";
  }
}

function getQuery(req) {
  try {
    const url = new URL(req.url || "/", "http://localhost");
    return Object.fromEntries(url.searchParams.entries());
  } catch {
    return {};
  }
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("error", reject);
    req.on("end", () => resolve(body));
  });
}

function checkAuth(req) {
  // If secret not configured -> open (NOT recommended)
  if (!CRAWLER_SECRET) return true;
  const auth = req.headers["authorization"] || "";
  if (typeof auth !== "string") return false;
  const m = auth.match(/^Bearer\s+(.+)$/i);
  if (!m) return false;
  return m[1].trim() === CRAWLER_SECRET;
}

function normalizeUrl(u) {
  try {
    const url = new URL(u);
    url.hash = "";
    url.search = "";
    url.hostname = url.hostname.replace(/^www\./, "");
    if (url.pathname.endsWith("/") && url.pathname !== "/") {
      url.pathname = url.pathname.slice(0, -1);
    }
    return url.toString();
  } catch {
    return u;
  }
}

function detectPageType(url = "", title = "") {
  const s = (url + " " + title).toLowerCase();
  if (/za-nas|about/.test(s)) return "about";
  if (/uslugi|services|pricing|price|ceni|tseni/.test(s)) return "services";
  if (/kontakti|contact/.test(s)) return "contact";
  if (/faq|vuprosi|questions/.test(s)) return "faq";
  if (/blog|news|article/.test(s)) return "blog";
  return "general";
}

// ================= BG NUMBER NORMALIZER =================
const BG_0_19 = [
  "нула","едно","две","три","четири","пет","шест","седем","осем","девет",
  "десет","единадесет","дванадесет","тринадесет","четиринадесет",
  "петнадесет","шестнадесет","седемнадесет","осемнадесет","деветнадесет"
];
const BG_TENS = ["", "", "двадесет","тридесет","четиридесет","петдесет","шестдесет","седемдесет","осемдесет","деветдесет"];

function numberToBgWords(n) {
  n = Number(n);
  if (Number.isNaN(n)) return n;
  if (n < 20) return BG_0_19[n];
  if (n < 100) {
    const t = Math.floor(n / 10);
    const r = n % 10;
    return BG_TENS[t] + (r ? " и " + BG_0_19[r] : "");
  }
  return String(n);
}

function normalizeNumbers(text = "") {
  try {
    return text.replace(
      /(\d+)\s?(лв|лева|€|eur|bgn|стая|стаи|човек|човека|нощувка|нощувки|кв\.?|sqm)/gi,
      (_, num, unit) => `${numberToBgWords(num)} ${unit}`
    );
  } catch {
    return text;
  }
}

// ================= SITEMAP EXTRACTION (kept) =================
const KEYWORD_MAP = {
  "резерв": ["book", "reserve", "booking"],
  "запази": ["book", "reserve"],
  "резервация": ["booking", "reservation"],
  "резервирай": ["book", "reserve"],
  "търси": ["search", "find"],
  "провери": ["check", "verify"],
  "покажи": ["show", "display"],
  "настаняване": ["check-in", "checkin", "arrival"],
  "напускане": ["check-out", "checkout", "departure"],
  "пристигане": ["arrival", "check-in"],
  "заминаване": ["departure", "check-out"],
  "контакт": ["contact"],
  "контакти": ["contact", "contacts"],
  "свържи": ["contact", "reach"],
  "стаи": ["rooms", "accommodation"],
  "стая": ["room"],
  "цени": ["prices", "rates"],
  "услуги": ["services"],
  "изпрати": ["send", "submit"],
};

function generateKeywords(text) {
  const lower = text.toLowerCase().trim();
  const keywords = new Set([lower]);
  const words = lower.split(/\s+/);
  words.forEach((w) => {
    if (w.length > 2) keywords.add(w);
  });
  for (const [bg, en] of Object.entries(KEYWORD_MAP)) {
    if (lower.includes(bg)) en.forEach((k) => keywords.add(k));
  }
  return Array.from(keywords).filter((k) => k.length > 1);
}

function detectActionType(text) {
  const lower = text.toLowerCase();
  if (/резерв|book|запази|reserve/i.test(lower)) return "booking";
  if (/контакт|contact|свържи/i.test(lower)) return "contact";
  if (/търси|search|провери|check|submit|изпрати/i.test(lower)) return "submit";
  if (/стаи|rooms|услуги|services|за нас|about|галерия|gallery/i.test(lower)) return "navigation";
  return "other";
}

function detectFieldType(name, type, placeholder, label) {
  const searchText = `${name} ${type} ${placeholder} ${label}`.toLowerCase();
  if (type === "date") return "date";
  if (type === "number") return "number";
  if (/date|дата/i.test(searchText)) return "date";
  if (/guest|човек|брой|count|number/i.test(searchText)) return "number";
  if (/select/i.test(type)) return "select";
  return "text";
}

function generateFieldKeywords(name, placeholder, label) {
  const keywords = new Set();
  const searchText = `${name} ${placeholder} ${label}`.toLowerCase();

  if (/check-?in|checkin|arrival|от|настаняване|пристигане|from|start/i.test(searchText)) {
    ["check-in", "checkin", "от", "настаняване", "arrival", "from"].forEach((k) => keywords.add(k));
  }
  if (/check-?out|checkout|departure|до|напускане|заминаване|to|end/i.test(searchText)) {
    ["check-out", "checkout", "до", "напускане", "departure", "to"].forEach((k) => keywords.add(k));
  }
  if (/guest|adult|човек|гост|брой|persons|pax/i.test(searchText)) {
    ["guests", "гости", "човека", "adults", "persons", "брой"].forEach((k) => keywords.add(k));
  }
  if (/name|име/i.test(searchText)) ["name", "име"].forEach((k) => keywords.add(k));
  if (/email|имейл|e-mail/i.test(searchText)) ["email", "имейл", "e-mail"].forEach((k) => keywords.add(k));
  if (/phone|телефон|тел/i.test(searchText)) ["phone", "телефон"].forEach((k) => keywords.add(k));

  if (name) keywords.add(name.toLowerCase());
  return Array.from(keywords);
}

async function extractSiteMapFromPage(page) {
  return await page.evaluate(() => {
    const getSelector = (el, idx) => {
      if (el.id) return `#${el.id}`;
      if (el.className && typeof el.className === "string") {
        const cls = el.className.trim().split(/\s+/)[0];
        if (cls && !cls.includes(":") && !cls.includes("[")) {
          const matches = document.querySelectorAll(`.${cls}`);
          if (matches.length === 1) return `.${cls}`;
        }
      }
      const tag = el.tagName.toLowerCase();
      const parent = el.parentElement;
      if (parent) {
        const siblings = Array.from(parent.children).filter((c) => c.tagName === el.tagName);
        const index = siblings.indexOf(el) + 1;
        if (el.className) {
          const cls = el.className.split(/\s+/)[0];
          if (cls) return `${tag}.${cls}`;
        }
        return `${tag}:nth-of-type(${index})`;
      }
      return `${tag}:nth-of-type(${idx + 1})`;
    };

    const isVisible = (el) => {
      const rect = el.getBoundingClientRect();
      const style = window.getComputedStyle(el);
      return rect.width > 0 && rect.height > 0 && style.display !== "none" && style.visibility !== "hidden";
    };

    const getLabel = (el) => {
      const id = el.id;
      if (id) {
        const label = document.querySelector(`label[for="${id}"]`);
        if (label) return label.textContent?.trim();
      }
      const parent = el.closest("label");
      if (parent) return parent.textContent?.trim();
      const prev = el.previousElementSibling;
      if (prev?.tagName === "LABEL") return prev.textContent?.trim();
      return "";
    };

    const buttons = [];
    const btnElements = document.querySelectorAll(
      "button, a[href], [role='button'], input[type='submit'], input[type='button'], .btn, .button"
    );

    btnElements.forEach((el, i) => {
      if (!isVisible(el)) return;
      const text = (el.textContent?.trim() || el.value || "").slice(0, 100);
      if (!text || text.length < 2) return;

      const href = el.href || "";
      if (/^(#|javascript:|mailto:|tel:)/.test(href)) return;
      if (href && !href.includes(window.location.hostname)) return;

      buttons.push({ text, selector: getSelector(el, i) });
    });

    const forms = [];
    document.querySelectorAll("form").forEach((form, formIdx) => {
      if (!isVisible(form)) return;

      const fields = [];
      form
        .querySelectorAll("input:not([type='hidden']):not([type='submit']), select, textarea")
        .forEach((input, inputIdx) => {
          if (!isVisible(input)) return;
          fields.push({
            name: input.name || input.id || `field_${inputIdx}`,
            selector: getSelector(input, inputIdx),
            type: input.type || input.tagName.toLowerCase(),
            placeholder: input.placeholder || "",
            label: getLabel(input),
          });
        });

      let submitSelector = "";
      const submitBtn = form.querySelector('button[type="submit"], input[type="submit"], button:not([type])');
      if (submitBtn) submitSelector = getSelector(submitBtn, 0);

      if (fields.length > 0) {
        forms.push({ selector: getSelector(form, formIdx), fields, submit_button: submitSelector });
      }
    });

    const prices = [];
    const priceRegex = /(\d+[\s,.]?\d*)\s*(лв\.?|BGN|EUR|€|\$|лева)/gi;

    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
    let node;
    while ((node = walker.nextNode())) {
      const text = node.textContent || "";
      const matches = [...text.matchAll(priceRegex)];
      matches.forEach((match) => {
        const parent = node.parentElement;
        let context = "";
        if (parent) {
          const container = parent.closest("div, article, section, li, tr");
          if (container) {
            const heading = container.querySelector("h1, h2, h3, h4, h5, h6, strong, b, .title");
            if (heading) context = heading.textContent?.trim().slice(0, 50) || "";
          }
        }
        if (!prices.some((p) => p.text === match[0] && p.context === context)) {
          prices.push({ text: match[0], context });
        }
      });
    }

    return { url: window.location.href, title: document.title, buttons: buttons.slice(0, 30), forms: forms.slice(0, 10), prices: prices.slice(0, 20) };
  });
}

function enrichSiteMap(raw, siteId, siteUrl) {
  return {
    site_id: siteId,
    url: siteUrl || raw.url || "",
    buttons: (raw.buttons || []).map((btn) => ({
      text: btn.text,
      selector: btn.selector,
      keywords: generateKeywords(btn.text),
      action_type: detectActionType(btn.text),
    })),
    forms: (raw.forms || []).map((form) => ({
      selector: form.selector,
      submit_button: form.submit_button,
      fields: form.fields.map((field) => ({
        name: field.name,
        selector: field.selector,
        type: detectFieldType(field.name, field.type, field.placeholder, field.label),
        keywords: generateFieldKeywords(field.name, field.placeholder, field.label),
      })),
    })),
    prices: (raw.prices || []).map((p) => ({ text: p.text, context: p.context || "" })),
  };
}

function buildCombinedSiteMap(pageSiteMaps, siteId, siteUrl) {
  const combined = { site_id: siteId, url: siteUrl, buttons: [], forms: [], prices: [] };
  const seenButtons = new Set();
  const seenForms = new Set();
  const seenPrices = new Set();

  for (const pageMap of pageSiteMaps) {
    for (const btn of pageMap.buttons || []) {
      const key = btn.text.toLowerCase();
      if (!seenButtons.has(key)) {
        seenButtons.add(key);
        combined.buttons.push(btn);
      }
    }
    for (const form of pageMap.forms || []) {
      const key = form.fields.map((f) => f.name).sort().join(",");
      if (!seenForms.has(key)) {
        seenForms.add(key);
        combined.forms.push(form);
      }
    }
    for (const price of pageMap.prices || []) {
      const key = `${price.text}|${price.context}`;
      if (!seenPrices.has(key)) {
        seenPrices.add(key);
        combined.prices.push(price);
      }
    }
  }

  combined.buttons = combined.buttons.slice(0, 50);
  combined.forms = combined.forms.slice(0, 15);
  combined.prices = combined.prices.slice(0, 30);

  console.log(`[SITEMAP] Combined: ${combined.buttons.length} buttons, ${combined.forms.length} forms, ${combined.prices.length} prices`);
  return combined;
}

async function sendSiteMapToWorker(siteMap) {
  if (!WORKER_URL || !WORKER_SECRET) {
    console.log("[SITEMAP] Worker not configured, skipping");
    return false;
  }

  try {
    console.log(`[SITEMAP] Sending to worker: ${WORKER_URL}/prepare-session`);
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 20000);

    const response = await fetch(`${WORKER_URL}/prepare-session`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${WORKER_SECRET}`,
      },
      body: JSON.stringify({ site_id: siteMap.site_id, site_map: siteMap }),
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (response.ok) {
      const result = await response.json().catch(() => ({}));
      console.log(`[SITEMAP] ✓ Worker response:`, result);
      return true;
    } else {
      const txt = await response.text().catch(() => "");
      console.error(`[SITEMAP] ✗ Worker error:`, response.status, txt.slice(0, 200));
      return false;
    }
  } catch (e) {
    console.error(`[SITEMAP] ✗ Worker send error:`, e?.message || e);
    return false;
  }
}

// ================= CONTENT EXTRACTION (kept) =================
async function extractStructured(page) {
  try {
    await page.waitForSelector("body", { timeout: 1500 });
  } catch {}

  try {
    return await page.evaluate(() => {
      const seenTexts = new Set();
      function addUniqueText(text, minLength = 10) {
        const normalized = text.trim().replace(/\s+/g, " ");
        if (normalized.length < minLength || seenTexts.has(normalized)) return "";
        seenTexts.add(normalized);
        return normalized;
      }

      const sections = [];
      let current = null;
      const processedElements = new Set();

      document.querySelectorAll("h1,h2,h3,p,li").forEach((el) => {
        if (processedElements.has(el)) return;
        let parent = el.parentElement;
        while (parent) {
          if (processedElements.has(parent)) return;
          parent = parent.parentElement;
        }
        const text = el.innerText?.trim();
        if (!text) return;
        const uniqueText = addUniqueText(text, 5);
        if (!uniqueText) return;
        if (el.tagName.startsWith("H")) {
          current = { heading: uniqueText, text: "" };
          sections.push(current);
        } else if (current) {
          current.text += " " + uniqueText;
        }
        processedElements.add(el);
      });

      let mainContent = "";
      const mainEl = document.querySelector("main") || document.querySelector("article");
      if (mainEl && !processedElements.has(mainEl)) {
        const text = mainEl.innerText?.trim();
        if (text) mainContent = addUniqueText(text) || "";
      }

      return { rawContent: [sections.map((s) => `${s.heading}\n${s.text}`).join("\n\n"), mainContent].filter(Boolean).join("\n\n") };
    });
  } catch {
    return { rawContent: "" };
  }
}

// ================= OCR =================
async function fastOCR(buffer) {
  try {
    if (!GOOGLE_VISION_API_KEY) return "";

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), OCR_TIMEOUT_MS);

    const res = await fetch(`https://vision.googleapis.com/v1/images:annotate?key=${GOOGLE_VISION_API_KEY}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        requests: [
          {
            image: { content: buffer.toString("base64") },
            features: [{ type: "TEXT_DETECTION" }],
            imageContext: { languageHints: ["bg", "en", "tr", "ru"] },
          },
        ],
      }),
      signal: controller.signal,
    });

    clearTimeout(timeout);
    if (!res.ok) return "";

    const json = await res.json();
    return json.responses?.[0]?.fullTextAnnotation?.text?.trim() || "";
  } catch {
    return "";
  }
}

async function ocrAllImages(page, stats) {
  const results = [];

  try {
    const imgElements = await page.$$("img");
    if (imgElements.length === 0) return results;

    const imgInfos = await Promise.all(
      imgElements.map(async (img, i) => {
        try {
          const info = await img.evaluate((el) => ({
            src: el.src || "",
            alt: el.alt || "",
            w: Math.round(el.getBoundingClientRect().width),
            h: Math.round(el.getBoundingClientRect().height),
            visible: el.getBoundingClientRect().width > 0,
          }));
          return { ...info, element: img, index: i };
        } catch {
          return null;
        }
      })
    );

    const validImages = imgInfos.filter((info) => {
      if (!info || !info.visible) return false;
      if (info.w < 50 || info.h < 25) return false;
      if (info.w > 1800 && info.h > 700) return false;
      if (SKIP_OCR_RE.test(info.src)) return false;
      return true;
    });

    console.log(`[OCR] ${validImages.length}/${imgElements.length} images to process`);
    if (validImages.length === 0) return results;

    const screenshots = await Promise.all(
      validImages.map(async (img) => {
        try {
          if (globalOcrCache.has(img.src)) {
            const cachedText = globalOcrCache.get(img.src);
            return { ...img, buffer: null, cached: true, text: cachedText };
          }
          if (page.isClosed()) return null;
          const buffer = await img.element.screenshot({ type: "png", timeout: 2500 });
          return { ...img, buffer, cached: false };
        } catch {
          return null;
        }
      })
    );

    const validScreenshots = screenshots.filter((s) => s !== null);

    for (let i = 0; i < validScreenshots.length; i += PARALLEL_OCR) {
      const batch = validScreenshots.slice(i, i + PARALLEL_OCR);

      const batchResults = await Promise.all(
        batch.map(async (img) => {
          if (img.cached) {
            if (img.text && img.text.length > 2) return { text: img.text, src: img.src, alt: img.alt };
            return null;
          }
          if (!img.buffer) return null;

          const text = await fastOCR(img.buffer);
          globalOcrCache.set(img.src, text);

          if (text && text.length > 2) {
            stats.ocrElementsProcessed++;
            stats.ocrCharsExtracted += text.length;
            return { text, src: img.src, alt: img.alt };
          }
          return null;
        })
      );

      results.push(...batchResults.filter((r) => r !== null));
    }
  } catch (e) {
    console.error("[OCR ERROR]", e?.message || e);
  }

  return results;
}

// ================= LINK DISCOVERY =================
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

// ================= PROCESS SINGLE PAGE =================
async function processPage(page, url, base, stats, siteMaps) {
  const startTime = Date.now();

  try {
    console.log("[PAGE]", url);
    await page.goto(url, { timeout: 10000, waitUntil: "domcontentloaded" });

    await page.evaluate(async () => {
      const scrollStep = window.innerHeight;
      const maxScroll = document.body.scrollHeight;
      for (let pos = 0; pos < maxScroll; pos += scrollStep) {
        window.scrollTo(0, pos);
        await new Promise((r) => setTimeout(r, 100));
      }
      window.scrollTo(0, maxScroll);

      document.querySelectorAll('img[loading="lazy"], img[data-src], img[data-lazy]').forEach((img) => {
        img.loading = "eager";
        if (img.dataset.src) img.src = img.dataset.src;
        if (img.dataset.lazy) img.src = img.dataset.lazy;
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
    } catch (e) {
      console.error("[SITEMAP] Extract error:", e?.message || e);
    }

    const htmlContent = normalizeNumbers(clean(data.rawContent));
    const ocrTexts = ocrResults.map((r) => r.text);
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

    if (pageType !== "services" && totalWords < MIN_WORDS) {
      return { links: await collectAllLinks(page, base), page: null };
    }

    return {
      links: await collectAllLinks(page, base),
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
  } catch (e) {
    console.error("[PAGE ERROR]", url, e?.message || e);
    stats.errors++;
    return { links: [], page: null };
  }
}

// ================= CRAWL =================
async function crawlSmart(startUrl, siteId = null) {
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
    byType: {},
    ocrElementsProcessed: 0,
    ocrCharsExtracted: 0,
    errors: 0,
  };

  const pages = [];
  const queue = [];
  const siteMaps = [];
  let base = "";

  try {
    const initContext = await browser.newContext({
      viewport: { width: 1920, height: 1080 },
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    });
    const initPage = await initContext.newPage();

    await initPage.goto(startUrl, { timeout: 10000, waitUntil: "domcontentloaded" });
    base = new URL(initPage.url()).origin;

    const initialLinks = await collectAllLinks(initPage, base);
    queue.push(normalizeUrl(initPage.url()));
    initialLinks.forEach((l) => {
      const nl = normalizeUrl(l);
      if (!visited.has(nl) && !SKIP_URL_RE.test(nl) && !queue.includes(nl)) queue.push(nl);
    });

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

        result.links.forEach((l) => {
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

  let combinedSiteMap = null;
  if (siteMaps.length > 0 && siteId) {
    console.log(`\n[SITEMAP] Building combined map from ${siteMaps.length} pages...`);
    const enrichedMaps = siteMaps.map((raw) => enrichSiteMap(raw, siteId, base));
    combinedSiteMap = buildCombinedSiteMap(enrichedMaps, siteId, base);
    // Worker push only (no Supabase dependency)
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

    console.log(`[REQ] ${reqId} ${req.method} ${path}`);

    // Health
    if (req.method === "GET" && path === "/health") return json(res, 200, { ok: true });

    // Status
    if (req.method === "GET" && (path === "/" || path === "/status")) {
      return json(res, 200, {
        ok: true,
        authEnabled: Boolean(CRAWLER_SECRET),
        ocrEnabled: Boolean(GOOGLE_VISION_API_KEY),
        workerEnabled: Boolean(WORKER_URL && WORKER_SECRET),
        jobsCount: jobs.size,
      });
    }

    // Result polling
    if (req.method === "GET" && path === "/result") {
      const job_id = q.job_id || "";
      if (!job_id) return json(res, 400, { ok: false, error: "Missing job_id" });

      const job = jobs.get(job_id);
      if (!job) return json(res, 404, { ok: false, error: "Job not found" });

      if (job.status === "queued" || job.status === "processing") {
        return json(res, 202, {
          ok: true,
          status: job.status,
          job_id,
          url: job.url,
          site_id: job.site_id,
        });
      }

      if (job.status === "failed") {
        return json(res, 500, {
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
      if (!checkAuth(req)) return json(res, 401, { ok: false, error: "Unauthorized" });

      let payload = {};
      try {
        const body = await readBody(req);
        payload = JSON.parse(body || "{}");
      } catch {
        return json(res, 400, { ok: false, error: "Invalid JSON" });
      }

      const url = typeof payload.url === "string" ? payload.url.trim() : "";
      const site_id = typeof payload.site_id === "string" ? payload.site_id.trim() : "";

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
    console.log("Crawler running on", PORT);
    console.log(`Config: ${PARALLEL_TABS} tabs, ${PARALLEL_OCR} parallel OCR`);
    console.log(`Worker: ${WORKER_URL || "(not set)"}`);
    console.log(`Auth: ${CRAWLER_SECRET ? "enabled" : "DISABLED"}`);
    console.log(`OCR: ${GOOGLE_VISION_API_KEY ? "enabled" : "DISABLED"}`);
  });

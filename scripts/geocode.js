import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(ROOT, "data");
const CSV_PATH = path.join(DATA_DIR, "chapters.csv");
const OUT_JSON_PATH = path.join(DATA_DIR, "chapters.json");
const CACHE_PATH = path.join(DATA_DIR, "geocode-cache.json");

// Be a good citizen: ~1 request/second
const REQUEST_DELAY_MS = 1100;

// Nominatim requires a User-Agent identifying your app.
// Put an email or URL (doesn't have to be fancy).
const USER_AGENT = "ChapterMap/1.0 (contact: you@example.com)";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseCsv(csvText) {
  const lines = csvText
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean);

  const header = lines.shift();
  if (!header) throw new Error("CSV is empty");

  const cols = header.split(",").map((c) => c.trim());
  const idx = (name) => cols.indexOf(name);

  const required = ["ChapterName", "City", "StateRegion", "Country"];
  for (const r of required) {
    if (idx(r) === -1) throw new Error(`CSV missing required column: ${r}`);
  }

  return lines.map((line) => {
    // Simple CSV split (works as long as you don't have commas inside values)
    const parts = line.split(",").map((p) => p.trim());
    return {
      ChapterName: parts[idx("ChapterName")] ?? "",
      City: parts[idx("City")] ?? "",
      StateRegion: parts[idx("StateRegion")] ?? "",
      Country: parts[idx("Country")] ?? ""
    };
  });
}

function normalizeKey({ City, StateRegion, Country }) {
  return [City, StateRegion, Country]
    .map((s) => (s || "").trim().toLowerCase())
    .filter(Boolean)
    .join("|");
}

async function geocodeNominatim({ City, StateRegion, Country }) {
  const qParts = [City, StateRegion, Country].filter((x) => x && x.trim());
  const q = qParts.join(", ");

  const url = new URL("https://nominatim.openstreetmap.org/search");
  url.searchParams.set("format", "jsonv2");
  url.searchParams.set("q", q);
  url.searchParams.set("limit", "1");

  const res = await fetch(url.toString(), {
    headers: {
      "User-Agent": USER_AGENT
    }
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Nominatim error ${res.status}: ${text.slice(0, 200)}`);
  }

  const data = await res.json();
  if (!Array.isArray(data) || data.length === 0) return null;

  const hit = data[0];
  const lat = Number(hit.lat);
  const lon = Number(hit.lon);

  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;

  return {
    lat,
    lng: lon,
    display_name: hit.display_name ?? "",
    importance: hit.importance ?? null
  };
}

function loadJsonIfExists(p, fallback) {
  if (!fs.existsSync(p)) return fallback;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function saveJson(p, obj) {
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}

async function main() {
  if (!fs.existsSync(CSV_PATH)) {
    throw new Error(`Missing ${CSV_PATH}. Create data/chapters.csv first.`);
  }

  const csv = fs.readFileSync(CSV_PATH, "utf8");
  const rows = parseCsv(csv);

  const cache = loadJsonIfExists(CACHE_PATH, {});
  const out = [];

  let geocodeCalls = 0;
  let cacheHits = 0;
  let failures = 0;

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];

    const key = normalizeKey(r);
    if (!key) {
      failures++;
      out.push({
        id: i + 1,
        chapterName: r.ChapterName,
        city: r.City,
        stateRegion: r.StateRegion,
        country: r.Country,
        lat: null,
        lng: null,
        geocodeNote: "Missing City/State/Country"
      });
      continue;
    }

    if (cache[key]) {
      cacheHits++;
      out.push({
        id: i + 1,
        chapterName: r.ChapterName,
        city: r.City,
        stateRegion: r.StateRegion,
        country: r.Country,
        lat: cache[key].lat,
        lng: cache[key].lng,
        geocodeNote: "cache"
      });
      continue;
    }

    // Call Nominatim
    geocodeCalls++;
    process.stdout.write(
      `Geocoding ${i + 1}/${rows.length}: ${r.City}, ${r.StateRegion}, ${r.Country} ... `
    );

    try {
      const result = await geocodeNominatim(r);
      if (!result) {
        failures++;
        console.log("NOT FOUND");
        out.push({
          id: i + 1,
          chapterName: r.ChapterName,
          city: r.City,
          stateRegion: r.StateRegion,
          country: r.Country,
          lat: null,
          lng: null,
          geocodeNote: "not found"
        });
      } else {
        console.log("OK");
        cache[key] = { lat: result.lat, lng: result.lng, display_name: result.display_name };
        out.push({
          id: i + 1,
          chapterName: r.ChapterName,
          city: r.City,
          stateRegion: r.StateRegion,
          country: r.Country,
          lat: result.lat,
          lng: result.lng,
          geocodeNote: "nominatim"
        });
      }
    } catch (e) {
      failures++;
      console.log("ERROR");
      out.push({
        id: i + 1,
        chapterName: r.ChapterName,
        city: r.City,
        stateRegion: r.StateRegion,
        country: r.Country,
        lat: null,
        lng: null,
        geocodeNote: `error: ${String(e.message || e)}`
      });
    }

    // throttle
    await sleep(REQUEST_DELAY_MS);
  }

  saveJson(CACHE_PATH, cache);
  saveJson(OUT_JSON_PATH, out);

  console.log("\nDone.");
  console.log(`Total chapters: ${rows.length}`);
  console.log(`Cache hits: ${cacheHits}`);
  console.log(`Geocode calls: ${geocodeCalls}`);
  console.log(`Failures (null lat/lng): ${failures}`);
  console.log(`Wrote: ${OUT_JSON_PATH}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

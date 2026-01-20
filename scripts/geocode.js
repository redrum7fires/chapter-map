/**
 * Batch geocode chapters.
 * Primary: Open-Meteo (FREE, no key)
 * Fallback (only when not found): Nominatim (OpenStreetMap)
 *
 * Input : data/chapters.csv
 * Output: data/chapters.json
 * Cache : data/geocode-cache.json
 *
 * Required CSV columns:
 *   ChapterName, City, StateRegion, Country
 *
 * Optional CSV columns (popup):
 *   PresidentName, PresidentCell, VicePresidentName, VicePresidentCell
 *
 * Run:
 *   node scripts/geocode.js
 */

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

// Throttle requests (be polite)
const REQUEST_DELAY_MS = 600;   // between chapter geocode attempts
const NOMINATIM_DELAY_MS = 900; // extra delay before Nominatim fallback

/* ----------------------------- helpers ----------------------------- */

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function loadJsonIfExists(filePath, fallback = {}) {
  if (!fs.existsSync(filePath)) return fallback;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function saveJson(filePath, data) {
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf8");
}

function norm(str) {
  return String(str ?? "").trim();
}

function normLower(str) {
  return norm(str).toLowerCase();
}

function normalizeCountryName(country) {
  const c = normLower(country);
  if (c === "usa" || c === "us" || c === "u.s." || c === "u.s.a.") return "United States";
  if (c === "uk" || c === "u.k.") return "United Kingdom";
  return norm(country);
}

function countryToISO2(country) {
  const c = normalizeCountryName(country).toLowerCase();
  if (c === "united states") return "US";
  if (c === "canada") return "CA";
  if (c === "australia") return "AU";
  if (c === "united kingdom") return "GB";
  if (c === "germany") return "DE";
  return "";
}

// Helps with Canadian county/station-style names (and similar)
function normalizePlaceName(city) {
  let c = norm(city);
  c = c.replace(/\bCounty\b/gi, "").trim();
  c = c.replace(/\bStation\b/gi, "").trim();
  c = c.replace(/\s{2,}/g, " ");
  return c;
}

// Expand common abbreviations to improve match quality
function expandCommonCityPrefixes(city) {
  const c = norm(city);

  // Only expand when it's a prefix; keep original too.
  const variants = new Set([c]);

  // Mt -> Mount
  if (/^Mt\s+/i.test(c)) variants.add(c.replace(/^Mt\s+/i, "Mount "));
  // St -> Saint
  if (/^St\s+/i.test(c)) variants.add(c.replace(/^St\s+/i, "Saint "));

  return Array.from(variants).filter(Boolean);
}

function makeCacheKey({ City, StateRegion, Country }) {
  const city = normalizePlaceName(City).toLowerCase();
  const state = normLower(StateRegion);
  const country = normLower(normalizeCountryName(Country));
  return [city, state, country].filter(Boolean).join("|");
}

/* ----------------------- robust CSV parsing ----------------------- */

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];

    if (ch === '"') {
      // Escaped quote ""
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      out.push(cur.trim());
      cur = "";
    } else {
      cur += ch;
    }
  }
  out.push(cur.trim());

  // Strip surrounding quotes if present
  return out.map(v => v.replace(/^"+|"+$/g, "").trim());
}

function parseCsv(csvText) {
  const lines = csvText
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean);

  if (lines.length === 0) throw new Error("chapters.csv is empty");

  const header = parseCsvLine(lines.shift());
  const idx = (name) => header.indexOf(name);

  const required = ["ChapterName", "City", "StateRegion", "Country"];
  for (const col of required) {
    if (idx(col) === -1) throw new Error(`CSV missing required column: ${col}`);
  }

  const optIndex = (col) => (idx(col) === -1 ? null : idx(col));
  const pName = optIndex("PresidentName");
  const pCell = optIndex("PresidentCell");
  const vpName = optIndex("VicePresidentName");
  const vpCell = optIndex("VicePresidentCell");

  return lines.map((line) => {
    const parts = parseCsvLine(line);
    const country = normalizeCountryName(parts[idx("Country")] || "");

    return {
      ChapterName: parts[idx("ChapterName")] || "",
      City: parts[idx("City")] || "",
      StateRegion: parts[idx("StateRegion")] || "",
      Country: country,

      PresidentName: pName !== null ? parts[pName] || "" : "",
      PresidentCell: pCell !== null ? parts[pCell] || "" : "",
      VicePresidentName: vpName !== null ? parts[vpName] || "" : "",
      VicePresidentCell: vpCell !== null ? parts[vpCell] || "" : ""
    };
  });
}

/* --------------------------- geocoding ---------------------------- */

// Open-Meteo search: return several hits so we can pick best match
async function openMeteoSearch(name, countryISO2 = "") {
  const url = new URL("https://geocoding-api.open-meteo.com/v1/search");
  url.searchParams.set("name", name);
  url.searchParams.set("count", "10");
  url.searchParams.set("language", "en");
  url.searchParams.set("format", "json");
  if (countryISO2) url.searchParams.set("country", countryISO2);

  const res = await fetch(url.toString());
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Open-Meteo error ${res.status}: ${text.slice(0, 200)}`);
  }

  const data = await res.json();
  if (!data?.results?.length) return [];

  return data.results
    .map((hit) => ({
      lat: Number(hit.latitude),
      lng: Number(hit.longitude),
      country: hit.country || "",
      admin1: hit.admin1 || "",
      name: hit.name || ""
    }))
    .filter((r) => Number.isFinite(r.lat) && Number.isFinite(r.lng));
}

function openMeteoPickBest(hits, expectedCountry, expectedAdmin1) {
  if (!hits || hits.length === 0) return null;

  const expC = normLower(expectedCountry);
  const expA = normLower(expectedAdmin1);

  // If we have expected state/admin1, require it.
  if (expA) {
    const match = hits.find(h =>
      normLower(h.country) === expC && normLower(h.admin1) === expA
    );
    if (match) return match;

    // If country is reliable but admin1 slightly different spelling, try loose match
    const loose = hits.find(h =>
      normLower(h.country) === expC && normLower(h.admin1).includes(expA)
    );
    if (loose) return loose;

    return null; // don't accept wrong state
  }

  // If no state provided, accept matching country first
  if (expC) {
    const match = hits.find(h => normLower(h.country) === expC);
    if (match) return match;
  }

  // Otherwise take first
  return hits[0];
}

// Nominatim fallback with address details so we can validate state/country
async function geocodeNominatim(query) {
  const url = new URL("https://nominatim.openstreetmap.org/search");
  url.searchParams.set("q", query);
  url.searchParams.set("format", "json");
  url.searchParams.set("limit", "5");
  url.searchParams.set("addressdetails", "1");

  const res = await fetch(url.toString(), {
    headers: {
      // Replace with your contact email (recommended by Nominatim usage guidance)
      "User-Agent": "ChapterMapPrototype/1.0 (contact: you@example.com)"
    }
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Nominatim error ${res.status}: ${text.slice(0, 200)}`);
  }

  const data = await res.json();
  if (!Array.isArray(data) || data.length === 0) return [];

  return data
    .map(hit => {
      const lat = Number(hit.lat);
      const lng = Number(hit.lon);
      const address = hit.address || {};
      return {
        lat,
        lng,
        country: address.country || hit.display_name || "",
        state: address.state || address.province || address.region || "",
        display: hit.display_name || ""
      };
    })
    .filter(r => Number.isFinite(r.lat) && Number.isFinite(r.lng));
}

function nominatimPickBest(hits, expectedCountry, expectedState) {
  if (!hits || hits.length === 0) return null;

  const expC = normLower(expectedCountry);
  const expS = normLower(expectedState);

  if (expS) {
    const match = hits.find(h =>
      normLower(h.country) === expC && normLower(h.state) === expS
    );
    if (match) return match;

    const loose = hits.find(h =>
      normLower(h.country) === expC && normLower(h.state).includes(expS)
    );
    if (loose) return loose;

    return null;
  }

  if (expC) {
    const match = hits.find(h => normLower(h.country) === expC);
    if (match) return match;
  }

  return hits[0];
}

async function geocodeWithFallback(row) {
  const countryName = normalizeCountryName(row.Country);
  const iso2 = countryToISO2(countryName);

  // normalize city and build variants (Mt -> Mount etc.)
  const cityRaw = normalizePlaceName(row.City);
  const cityVariants = expandCommonCityPrefixes(cityRaw);

  const state = norm(row.StateRegion);

  if (!cityRaw || !countryName) return null;

  // Query candidates: try with state+country, then country only
  const candidateQueries = [];
  for (const cv of cityVariants) {
    candidateQueries.push([cv, state, countryName].filter(Boolean).join(", "));
    candidateQueries.push([cv, countryName].filter(Boolean).join(", "));
  }

  // Remove duplicates
  const candidates = Array.from(new Set(candidateQueries)).filter(Boolean);

  // 1) Open-Meteo: try with country filter, then without
  for (const q of candidates) {
    const tries = iso2 ? [{ q, iso2 }, { q, iso2: "" }] : [{ q, iso2: "" }];

    for (const t of tries) {
      const hits = await openMeteoSearch(t.q, t.iso2);
      const best = openMeteoPickBest(hits, countryName, state);
      if (best) return { lat: best.lat, lng: best.lng, source: "open-meteo", queryUsed: t.q };
    }
  }

  // 2) Nominatim fallback (only for misses)
  await sleep(NOMINATIM_DELAY_MS);

  for (const q of candidates) {
    const hits = await geocodeNominatim(q);
    const best = nominatimPickBest(hits, countryName, state);
    if (best) return { lat: best.lat, lng: best.lng, source: "nominatim", queryUsed: q };
  }

  return null;
}

/* ------------------------------ main ------------------------------ */

async function main() {
  if (!fs.existsSync(CSV_PATH)) throw new Error("data/chapters.csv not found");

  const csvText = fs.readFileSync(CSV_PATH, "utf8");
  const rows = parseCsv(csvText);

  const cache = loadJsonIfExists(CACHE_PATH, {});
  const output = [];

  let cacheHits = 0;
  let apiCalls = 0;
  let failures = 0;

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const key = makeCacheKey(row);

    const baseOut = {
      id: i + 1,
      chapterName: row.ChapterName,
      city: row.City,
      stateRegion: row.StateRegion,
      country: row.Country,

      presidentName: row.PresidentName,
      presidentCell: row.PresidentCell,
      vicePresidentName: row.VicePresidentName,
      vicePresidentCell: row.VicePresidentCell
    };

    if (!key) {
      failures++;
      output.push({ ...baseOut, lat: null, lng: null, geocodeNote: "missing location data" });
      continue;
    }

    if (cache[key]) {
      cacheHits++;
      output.push({ ...baseOut, lat: cache[key].lat, lng: cache[key].lng, geocodeNote: "cache" });
      continue;
    }

    process.stdout.write(
      `Geocoding ${i + 1}/${rows.length}: ${row.ChapterName} — ${row.City}, ${row.StateRegion}, ${row.Country} ... `
    );

    try {
      apiCalls++;
      const result = await geocodeWithFallback(row);

      if (!result) {
        failures++;
        console.log("NOT FOUND");
        output.push({ ...baseOut, lat: null, lng: null, geocodeNote: "not found" });
      } else {
        console.log(`OK (${result.source})`);
        cache[key] = { lat: result.lat, lng: result.lng };
        output.push({ ...baseOut, lat: result.lat, lng: result.lng, geocodeNote: result.source });
      }
    } catch (err) {
      failures++;
      console.log("ERROR");
      output.push({
        ...baseOut,
        lat: null,
        lng: null,
        geocodeNote: `error: ${err.message}`
      });
    }

    await sleep(REQUEST_DELAY_MS);
  }

  saveJson(CACHE_PATH, cache);
  saveJson(OUT_JSON_PATH, output);

  console.log("\nGeocoding complete.");
  console.log(`Total chapters : ${rows.length}`);
  console.log(`Cache hits     : ${cacheHits}`);
  console.log(`API calls      : ${apiCalls}`);
  console.log(`Failures       : ${failures}`);
  console.log(`Output written : ${OUT_JSON_PATH}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

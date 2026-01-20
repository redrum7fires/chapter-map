/**
 * Batch geocode chapters.
 * Primary: Open-Meteo (FREE, no key)
 * Fallback (only when not found): Nominatim (OpenStreetMap) with polite headers + throttle
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
const REQUEST_DELAY_MS = 600;         // between geocode attempts
const NOMINATIM_DELAY_MS = 900;       // extra delay before Nominatim fallback

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

function normalizeCountryName(country) {
  const c = (country || "").trim().toLowerCase();
  if (c === "usa" || c === "us" || c === "u.s." || c === "u.s.a.") return "United States";
  if (c === "uk" || c === "u.k.") return "United Kingdom";
  return (country || "").trim();
}

// ISO2 for Open-Meteo `country=` filter
function countryToISO2(country) {
  const c = normalizeCountryName(country).toLowerCase();
  if (c === "united states") return "US";
  if (c === "canada") return "CA";
  if (c === "australia") return "AU";
  if (c === "united kingdom") return "GB";
  if (c === "germany") return "DE";
  return "";
}

// Helps with Canadian county/station-style names
function normalizePlaceName(city) {
  let c = (city || "").trim();
  c = c.replace(/\bCounty\b/gi, "").trim();
  c = c.replace(/\bStation\b/gi, "").trim();
  c = c.replace(/\s{2,}/g, " ");
  return c;
}

function parseCsv(csvText) {
  const lines = csvText
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean);

  if (lines.length === 0) throw new Error("chapters.csv is empty");

  const header = lines.shift().split(",").map((h) => h.trim());
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
    // NOTE: Assumes no commas inside values (no quoted commas).
    const parts = line.split(",").map((p) => p.trim());

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

function makeCacheKey({ City, StateRegion, Country }) {
  const city = normalizePlaceName(City).toLowerCase();
  const state = (StateRegion || "").trim().toLowerCase();
  const country = normalizeCountryName(Country).toLowerCase();
  return [city, state, country].filter(Boolean).join("|");
}

/* --------------------------- geocoding ---------------------------- */

// Open-Meteo search: return several hits so we can pick best match
async function openMeteoSearch(name, countryISO2 = "") {
  const url = new URL("https://geocoding-api.open-meteo.com/v1/search");
  url.searchParams.set("name", name);
  url.searchParams.set("count", "5");
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

// Nominatim fallback (only used when Open-Meteo fails)
async function geocodeNominatim(query) {
  const url = new URL("https://nominatim.openstreetmap.org/search");
  url.searchParams.set("q", query);
  url.searchParams.set("format", "json");
  url.searchParams.set("limit", "1");

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
  if (!Array.isArray(data) || data.length === 0) return null;

  const hit = data[0];
  const lat = Number(hit.lat);
  const lng = Number(hit.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;

  return { lat, lng };
}

async function geocodeWithFallback(row) {
  const city = normalizePlaceName(row.City);
  const state = (row.StateRegion || "").trim();
  const countryName = normalizeCountryName(row.Country);
  const iso2 = countryToISO2(countryName);

  if (!city || !countryName) return null;

  // Try stricter queries first
  const candidates = [
    [city, state, countryName].filter(Boolean).join(", "),
    [city, countryName].filter(Boolean).join(", "),
    city
  ].filter(Boolean);

  // 1) Open-Meteo (with country filter if possible, then without)
  for (const q of candidates) {
    const tries = iso2 ? [{ q, iso2 }, { q, iso2: "" }] : [{ q, iso2: "" }];

    for (const t of tries) {
      const hits = await openMeteoSearch(t.q, t.iso2);
      if (hits.length > 0) {
  const expectedState = (row.StateRegion || "").trim().toLowerCase();
  const expectedCountry = (row.Country || "").trim().toLowerCase();

  // Prefer exact match on country + admin1/state when state is provided
  const best = hits.find(h =>
    (h.country || "").toLowerCase() === expectedCountry &&
    (!expectedState || (h.admin1 || "").toLowerCase() === expectedState)
  ) || hits.find(h =>
    // fallback: at least match country
    (h.country || "").toLowerCase() === expectedCountry
  ) || hits[0];

  return { lat: best.lat, lng: best.lng, source: "open-meteo" };
}

    }
  }

  // 2) Nominatim fallback (only for misses)
  await sleep(NOMINATIM_DELAY_MS);

  const fallbackQueries = [
    [city, state, countryName].filter(Boolean).join(", "),
    [city, countryName].filter(Boolean).join(", ")
  ];

  for (const q of fallbackQueries) {
    const r = await geocodeNominatim(q);
    if (r) return { ...r, source: "nominatim", queryUsed: q };
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
      `Geocoding ${i + 1}/${rows.length}: ${row.City}, ${row.StateRegion}, ${row.Country} ... `
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

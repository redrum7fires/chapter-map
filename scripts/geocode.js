/**
 * Batch geocode chapters using Open-Meteo (FREE, no API key).
 * Input : data/chapters.csv
 * Output: data/chapters.json
 * Cache : data/geocode-cache.json
 *
 * Run with:
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

// Gentle throttle (Open-Meteo is generous, but we’ll be polite)
const REQUEST_DELAY_MS = 500;

/* --------------------------------------------------------- */
/* Utilities */
/* --------------------------------------------------------- */

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

/* --------------------------------------------------------- */
/* CSV parsing */
/* --------------------------------------------------------- */

function parseCsv(csvText) {
  const lines = csvText
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean);

  if (lines.length === 0) {
    throw new Error("chapters.csv is empty");
  }

  const header = lines.shift().split(",").map((h) => h.trim());
  const idx = (name) => header.indexOf(name);

  const required = ["ChapterName", "City", "StateRegion", "Country"];
  for (const col of required) {
    if (idx(col) === -1) {
      throw new Error(`CSV missing required column: ${col}`);
    }
  }

  return lines.map((line) => {
    const parts = line.split(",").map((p) => p.trim());
    return {
      ChapterName: parts[idx("ChapterName")] || "",
      City: parts[idx("City")] || "",
      StateRegion: parts[idx("StateRegion")] || "",
      Country: parts[idx("Country")] || ""
    };
  });
}

function makeCacheKey({ City, StateRegion, Country }) {
  return [City, StateRegion, Country]
    .map((v) => (v || "").toLowerCase().trim())
    .filter(Boolean)
    .join("|");
}

/* --------------------------------------------------------- */
/* Open-Meteo geocoding */
/* --------------------------------------------------------- */

async function geocodeOpenMeteo({ City, StateRegion, Country }) {
  const name = [City, StateRegion].filter(Boolean).join(", ");

  const url = new URL("https://geocoding-api.open-meteo.com/v1/search");
  url.searchParams.set("name", name);
  url.searchParams.set("count", "1");
  url.searchParams.set("language", "en");
  url.searchParams.set("format", "json");

  const res = await fetch(url.toString());
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Open-Meteo error ${res.status}: ${text.slice(0, 200)}`);
  }

  const data = await res.json();
  if (!data?.results?.length) return null;

  const hit = data.results[0];
  const lat = Number(hit.latitude);
  const lng = Number(hit.longitude);

  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;

  return {
    lat,
    lng,
    display_name: hit.name || ""
  };
}

/* --------------------------------------------------------- */
/* Main */
/* --------------------------------------------------------- */

async function main() {
  if (!fs.existsSync(CSV_PATH)) {
    throw new Error("data/chapters.csv not found");
  }

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

    if (!key) {
      failures++;
      output.push({
        id: i + 1,
        chapterName: row.ChapterName,
        city: row.City,
        stateRegion: row.StateRegion,
        country: row.Country,
        lat: null,
        lng: null,
        geocodeNote: "missing location data"
      });
      continue;
    }

    if (cache[key]) {
      cacheHits++;
      output.push({
        id: i + 1,
        chapterName: row.ChapterName,
        city: row.City,
        stateRegion: row.StateRegion,
        country: row.Country,
        lat: cache[key].lat,
        lng: cache[key].lng,
        geocodeNote: "cache"
      });
      continue;
    }

    process.stdout.write(
      `Geocoding ${i + 1}/${rows.length}: ${row.City}, ${row.StateRegion}, ${row.Country} ... `
    );

    try {
      apiCalls++;
      const result = await geocodeOpenMeteo(row);

      if (!result) {
        failures++;
        console.log("NOT FOUND");
        output.push({
          id: i + 1,
          chapterName: row.ChapterName,
          city: row.City,
          stateRegion: row.StateRegion,
          country: row.Country,
          lat: null,
          lng: null,
          geocodeNote: "not found"
        });
      } else {
        console.log("OK");
        cache[key] = { lat: result.lat, lng: result.lng };
        output.push({
          id: i + 1,
          chapterName: row.ChapterName,
          city: row.City,
          stateRegion: row.StateRegion,
          country: row.Country,
          lat: result.lat,
          lng: result.lng,
          geocodeNote: "open-meteo"
        });
      }
    } catch (err) {
      failures++;
      console.log("ERROR");
      output.push({
        id: i + 1,
        chapterName: row.ChapterName,
        city: row.City,
        stateRegion: row.StateRegion,
        country: row.Country,
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

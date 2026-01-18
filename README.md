# Chapter Map (Leaflet + OpenStreetMap)

## Prereqs
- Node 18+ installed

## Steps
1) Put your chapters in `data/chapters.csv`

2) Run geocoding (creates data/chapters.json)
   - Edit USER_AGENT in scripts/geocode.js first
   - Then:
     npm run geocode

3) Serve the map:
   npm run serve

Open:
http://localhost:8080

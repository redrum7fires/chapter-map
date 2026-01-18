import http from "http";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ROOT = path.resolve(__dirname, "..");
const WEB = path.join(ROOT, "web");
const DATA = path.join(ROOT, "data");

const port = 8080;

const server = http.createServer((req, res) => {
  const url = (req.url || "/").split("?")[0];

  // Map /chapters.json to data/chapters.json
  if (url === "/chapters.json") {
    const p = path.join(DATA, "chapters.json");
    if (!fs.existsSync(p)) {
      res.writeHead(404, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "chapters.json not found. Run: npm run geocode" }));
      return;
    }
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(fs.readFileSync(p));
    return;
  }

  const filePath = url === "/" ? path.join(WEB, "index.html") : path.join(WEB, url);
  if (!filePath.startsWith(WEB)) {
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }

  if (!fs.existsSync(filePath)) {
    res.writeHead(404);
    res.end("Not found");
    return;
  }

  const ext = path.extname(filePath).toLowerCase();
  const contentType =
    ext === ".html" ? "text/html"
    : ext === ".css" ? "text/css"
    : ext === ".js" ? "text/javascript"
    : "application/octet-stream";

  res.writeHead(200, { "Content-Type": contentType });
  res.end(fs.readFileSync(filePath));
});

server.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
  console.log(`- Map: http://localhost:${port}/`);
  console.log(`- Data: http://localhost:${port}/chapters.json`);
});

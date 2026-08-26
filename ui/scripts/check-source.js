import { access, readFile } from "node:fs/promises";

const required = [
  "src/app/bootstrap.ts",
  "src/stores/runtime-store.ts",
  "src/commands/bridge.ts",
  "src/charts/market-chart.ts",
  "src/layouts/workspace.ts",
  "src/app/main.ts",
  "src/theme/tokens.css",
  "src-tauri/Cargo.toml",
  "src-tauri/tauri.conf.json",
  "src-tauri/src/lib.rs",
  "src-tauri/src/main.rs",
  "vite.config.js",
  "index.html",
  "package.json",
];
await Promise.all(required.map((path) => access(new URL(`../${path}`, import.meta.url))));
const packageJson = JSON.parse(await readFile(new URL("../package.json", import.meta.url)));
if (packageJson.private !== true || packageJson.type !== "module") {
  throw new Error("UI package must remain private and use ES modules");
}

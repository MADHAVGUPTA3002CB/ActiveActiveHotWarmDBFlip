import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("builds the Flipbench control room server bundle", async () => {
  const [page, dashboard, bundle] = await Promise.all([
    readFile(new URL("../app/page.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/playground.tsx", import.meta.url), "utf8"),
    readFile(new URL("../dist/server/index.js", import.meta.url), "utf8"),
  ]);
  assert.match(page, /title: "Flipbench Control Room"/i);
  assert.match(page, /Live PostgreSQL, Debezium and Kafka hot-to-warm flip playground/i);
  assert.match(dashboard, /Connecting to Flipbench/i);
  assert.match(dashboard, /localhost:8090/i);
  assert.match(bundle, /generateStaticParamsMap/);
});

test("keeps the live dashboard and safety copy in the application", async () => {
  const [page, dashboard, css] = await Promise.all([
    readFile(new URL("../app/page.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/playground.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
  ]);
  assert.match(page, /<Playground \/>/);
  assert.match(dashboard, /WAL → Debezium lag/);
  assert.match(dashboard, /Kafka → warm lag/);
  assert.match(dashboard, /Start flip/);
  assert.match(dashboard, /Apply live settings/);
  assert.match(dashboard, /stable samples/i);
  assert.match(dashboard, /New experiment/);
  assert.match(dashboard, /Saved experiment history/);
  assert.match(dashboard, /Saved timing breakdown/);
  assert.match(dashboard, /Source fence proof/);
  assert.match(dashboard, /Warm sink proof/);
  assert.match(dashboard, /Ownership grant/);
  assert.match(dashboard, /type RESET/i);
  assert.match(dashboard, /localhost:8091/i);
  assert.match(dashboard, /Control API.*localhost:8090/i);
  assert.match(dashboard, /make playground-api-rf3/i);
  assert.match(dashboard, /volumes may already have changed/i);
  assert.match(css, /\.pipeline/);
  assert.match(css, /\.modal-backdrop/);
  assert.match(css, /\.history-card/);
  assert.match(css, /@media \(max-width: 800px\)/);
});

# Flipbench playground UI

Local-only control room for the PostgreSQL → Debezium → Kafka → PostgreSQL flip prototype.

The UI expects the control API at `http://localhost:8090` and is intentionally not configured for hosting: its mutation endpoints control a loopback-only local benchmark stack.

```bash
npm install
npm run dev
```

Verification:

```bash
npm test
npm run lint
```

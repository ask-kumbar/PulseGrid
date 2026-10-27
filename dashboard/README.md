# PulseGrid dashboard

React and TypeScript UI for the Fleet Signal Monitor, built with Vite and Recharts. It reads the real Query API and displays device selection, latest temperature/humidity, recent history, and persisted active alerts.

## Run during development

Keep the Query API running on port `8001`. From this directory:

```bash
npm ci
npm run dev
```

Open [localhost:5173](http://localhost:5173). For the backend startup commands, use the [root README](../README.md) or [command guide](../docs/command-guide.md).

The browser requests `/api/v1/...`; Vite forwards those requests to `http://127.0.0.1:8001/v1/...`. That proxy exists only during Vite development. The full Docker demo serves built files and forwards `/api` through its web server instead.

## Configuration

Defaults target tenant `acme` and the initial device `sensor-204`. Optional `VITE_TENANT_ID` and `VITE_DEVICE_ID` settings are described in `.env.example`; the dashboard also discovers devices through the API.

`VITE_QUERY_API_URL` can point to a different Query API base URL. Using a different browser origin requires that API to allow the dashboard's origin through CORS. Vite environment values are included in the browser bundle at build time and must never contain secrets. Restart the Vite server after changing its environment file.

The dashboard refreshes every five seconds. Charts cover a 24-hour query window with a bounded number of history rows. Alerts are active breach records; they are not automatically resolved when a reading returns to normal.

## Source layout

| File | Purpose |
| --- | --- |
| `index.html` | HTML entry point and React mount element |
| `src/main.tsx` | Mount the React application |
| `src/App.tsx` | Dashboard state, refresh behavior, cards, charts, and alerts |
| `src/api.ts` | HTTP requests to the Query API |
| `src/types.ts` | TypeScript response types |
| `src/index.css` | Global typography and base styles |
| `src/App.css` | Dashboard layout and component styling |
| `vite.config.ts` | Local development proxy |

## Check and build

```bash
npm run lint
npm run build
```

The build checks TypeScript and writes browser assets to `dist/`. Serving those files alone does not create an API proxy. Use the repository's full Docker demo for the complete built application:

```bash
cd ..
docker compose -f compose.demo.yaml up --build -d --wait
```

Then open [localhost:8080](http://localhost:8080) and send simulator events using the root README commands. Public hosting is still pending.

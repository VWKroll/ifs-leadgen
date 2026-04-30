# Smoke Test Checklist

Run the app from the repo root:

```bash
npm run dev
```

Then verify:

- Dashboard loads at `http://127.0.0.1:3000` without `Failed to fetch`.
- Opportunity list populates and the first cluster auto-selects.
- Switching between several clusters updates the detail panel without showing false API errors.
- Map view loads, markers render, and selecting a map event updates the selected cluster.
- Graph view renders for the selected cluster.
- `Selected Cluster` chat returns a response for a simple prompt like `Summarize this cluster`.
- `Knowledge Base` chat either responds normally or shows a clear readiness message instead of hanging.
- Settings page loads pipeline settings and recent generation runs.
- Starting a generation run creates a new run record and the status refreshes.

Optional checks:

- Add `NEXT_PUBLIC_MAPBOX_TOKEN` in `frontend/.env.local` and confirm live map geocoding works.
- Restart `npm run dev` after updating `backend/.env` and confirm chat still works.

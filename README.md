---
title: NYC Intel
emoji: 🗽
colorFrom: blue
colorTo: yellow
sdk: docker
app_port: 7860
pinned: false
---

# NYC Intel

> An AI-powered urban intelligence platform that lets anyone ask natural-language questions about New York City and get back charts, maps, and insights in seconds.

**Live demo:** [https://huggingface.co/spaces/ArchitBhujang/nyc_intel](https://huggingface.co/spaces/ArchitBhujang/nyc_intel)

Ask questions like *"Which boroughs have the most property crime?"* or *"Rank neighborhoods by vulnerability and crime density"* and the app translates your English into SQL, runs it against 579K+ records on Databricks, and renders the result as interactive visualizations.

---

## What It Does

NYC Intel turns New York City's open data into a conversation. Users type a question in plain English — no SQL, no dashboards to configure, no filters to set — and the system returns:

- A **plain-English answer** summarizing the key finding
- **KPI cards** highlighting the headline numbers
- **Interactive charts** (bar, pie, polar, horizontal) automatically chosen based on the data shape
- A **live map** with coordinates, borough breakdowns, or heatmaps where relevant
- The **generated SQL** so you can verify what was actually run
- A **raw data table** with all returned rows

Behind the scenes, five NYC datasets are linked through spatial reverse-geocoding so questions can cross domains — for example, combining crime records with social vulnerability and bus stop density in a single query.

---

## Why I Built It

NYC publishes incredible open data — the NYPD complaint database alone has over half a million records — but almost no one uses it because querying it requires knowing SQL, knowing the schema, and having the infrastructure to run joins across millions of rows.

I wanted to build something that removes all of that friction. A city planner, researcher, or curious resident should be able to ask *"where should the city prioritize new streetlights?"* and get a real answer backed by real data, not a PDF report from 2019.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | Single-file HTML/CSS/JS, Chart.js, Leaflet.js with Leaflet.heat |
| Backend | Python 3.11, FastAPI, Uvicorn |
| AI / NLP | OpenAI GPT-4o-mini via the Responses API |
| Data warehouse | Databricks SQL Warehouse (serverless) |
| Data pipeline | PySpark notebooks on Databricks for ingestion and spatial joins |
| Deployment | Docker container on Hugging Face Spaces |
| Typography | Fraunces (display), Plus Jakarta Sans (body), JetBrains Mono (code) |

---

## The Datasets

All five tables are linked through ZIP code, enabling cross-dataset queries that would otherwise require writing custom ETL.

**1. `new_york_crime`** — 579K+ NYPD complaint records with offense type, severity, coordinates, demographics, and spatially-joined ZIP codes.

**2. `new_york_svi`** — CDC Social Vulnerability Index at the ZIP level. Includes poverty rate, unemployment, housing burden, language barriers, and an overall `RPL_THEMES` score from 0 to 1.

**3. `new_york_pop_zip`** — Population estimates per Modified ZIP Code for computing per-capita rates.

**4. `new_york_grocery_coords`** — Grocery store locations across NYC for food-access analysis.

**5. `new_york_bus`** — 3,381 MTA bus stops with shelter info, FEMA flood zones, and hurricane evacuation data.

### The Spatial Join

Crime records and bus stops originally came with lat/lng but no ZIP code. To make them joinable with SVI and population, I wrote a PySpark job that:

1. Loaded the NYC MODZCTA GeoJSON (ZIP polygon boundaries)
2. Converted each record's lat/lng into a Shapely point
3. Used a broadcast spatial join to find the containing polygon
4. Wrote the resulting `MODZCTA` column back to the Delta tables

This one preprocessing step is what makes questions like *"crime per capita by ZIP"* answerable in a single SQL query.

---

## How It Works

The request lifecycle from question to answer:

**1. User types a question** in the frontend and clicks Ask.

**2. Frontend sends JSON** to the FastAPI `/ask` endpoint: `{"question": "...", "max_rows": 50}`.

**3. Backend builds a SQL prompt** containing the full schema, join rules, column casting notes, and strict output rules. This prompt is ~200 lines and is the most important piece of the system — it's what keeps the model honest about which columns exist and how to cast messy ones (like `pop_est` which is stored as a string with commas).

**4. GPT-4o-mini generates one SELECT statement.** Temperature is zero for determinism.

**5. The SQL is sanitized** — any non-SELECT keyword (INSERT, DROP, etc.) triggers an immediate 400 error. Belt-and-suspenders even though the Databricks token is scoped read-only.

**6. Databricks executes the query.** If it fails with a `MISSING_AGGREGATION` error (a common GPT mistake), the backend parses the error, extracts the problematic column, adds it to the GROUP BY clause, and retries — up to 5 times. This auto-repair loop handles ~90% of the remaining failures.

**7. GPT summarizes the result** in plain English with 1–3 key takeaways.

**8. Response returns to the frontend** as `{sql, rows, answer}`.

**9. Frontend analyzes the shape of the data** and picks visualizations:
- 1 row with multiple numeric columns → doughnut chart
- Few rows with one label + one number → bar + pie + polar
- Many rows with coordinates → Leaflet map with heatmap overlay
- Borough names detected → proportional bubble map over NYC
- Anything else → sortable data table

The whole round trip usually takes 2–4 seconds.

---

## Project Structure

```
nyc_intel/
├── Dockerfile              # HF Spaces container definition
├── README.md               # HF Space config (front matter) + this file
├── requirements.txt        # Pinned Python dependencies
├── main.py                 # FastAPI app: schema prompt, SQL repair, /ask endpoint
└── static/
    └── index.html          # Single-file frontend (HTML + CSS + JS)
```

No build step, no bundler, no framework. One Python file, one HTML file.

---

## Running Locally

**Prerequisites:** Python 3.11+, a Databricks workspace with the five tables loaded, an OpenAI API key.

```bash
git clone https://github.com/BhujArc24/nyc_intel.git
cd nyc_intel
pip install -r requirements.txt
```

Create a `.env` file in the root:

```
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o-mini
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_TOKEN=dapi...
```

Then run:

```bash
uvicorn main:app --reload --port 8000
```

Open [http://localhost:8000](http://localhost:8000) in your browser.

---

## Deployment

The app runs on Hugging Face Spaces using a Docker container. Secrets are injected as environment variables through the HF Spaces UI — no keys are committed to the repo.

To redeploy, any push to the HF remote triggers a rebuild:

```bash
git push
```

Build takes 3–5 minutes.

---

## Example Questions It Can Answer

- *Which boroughs have the highest crime levels?*
- *Show the top 5 ZIP codes by social vulnerability score*
- *What are the most common crime types in Manhattan?*
- *Rank neighborhoods by bus stop density and grocery proximity*
- *Which ZIPs have high vulnerability AND high property crime?*
- *Show crime counts by law category — felony, misdemeanor, violation*
- *Which boroughs have the most shelters and highest flood risk?*
- *Crime per capita by ZIP code*

---

## What I Learned

- **Prompt engineering is the real engineering.** The schema prompt went through ~30 iterations. Every edge case (string columns that look numeric, tables with slightly different borough casing, aggregation gotchas) had to be surfaced in the prompt before GPT would write correct SQL reliably.
- **Auto-retry is underrated.** The `MISSING_AGGREGATION` repair loop turned an unreliable system into a reliable one with ~40 lines of code.
- **Serverless warehouses have cold starts.** First query of the day takes 15–30 seconds because the cluster wakes up. Subsequent queries are sub-second.
- **Databricks spatial joins at scale are non-obvious.** Broadcasting the small polygon table and using Shapely inside a PySpark UDF was ~50x faster than any geopandas approach I tried.

---

## Credits

Built by **Archit Bhujang** · [LinkedIn](https://www.linkedin.com/in/archit-bhujang-840b63217/) · [GitHub](https://github.com/BhujArc24)

Data sources: [NYC OpenData](https://opendata.cityofnewyork.us/), [CDC SVI](https://www.atsdr.cdc.gov/placeandhealth/svi/), [US Census Bureau](https://www.census.gov/).
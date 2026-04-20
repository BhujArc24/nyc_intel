# main.py
import os
import re
from typing import List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

from openai import OpenAI
from databricks import sql

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY not set")

client = OpenAI(api_key=OPENAI_API_KEY)

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

if not (DATABRICKS_HOST and DATABRICKS_HTTP_PATH and DATABRICKS_TOKEN):
    raise RuntimeError("Databricks settings missing")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Serve frontend ---
STATIC_DIR = Path(__file__).parent / "static"

@app.get("/")
async def root():
    return FileResponse(STATIC_DIR / "index.html")

# (rest of your existing code — unchanged)

class AskRequest(BaseModel):
    question: str
    max_rows: int = 50


def run_sql(query: str) -> List[dict]:
    with sql.connect(
        server_hostname=DATABRICKS_HOST.replace("https://", "").replace("http://", ""),
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [c[0] for c in cur.description] if cur.description else []
            rows = cur.fetchall()
            return [dict(zip(cols, r)) for r in rows]


def sanitize_sql(sql_text: str) -> str:
    sql_text = sql_text.strip().rstrip(";")
    lower = sql_text.lower()
    forbidden = ["insert ", "update ", "delete ", "drop ", "alter ", "create ", "truncate ", "grant ", "revoke "]
    for f in forbidden:
        if f in lower:
            raise HTTPException(status_code=400, detail=f"Forbidden SQL keyword: {f.strip()}")
    if not lower.lstrip().startswith("select"):
        raise HTTPException(status_code=400, detail="Query must be SELECT")
    return sql_text


def build_human_answer(question: str, sql_query: str, rows: List[dict]) -> str:
    prompt = f"""Answer this question conversationally in 2-3 short sentences, like you're explaining to a friend. Be specific with numbers but natural.

User asked: {question}

Data returned:
{rows}

Write a natural, friendly response that:
- Leads with the most interesting finding
- Uses specific numbers from the data
- Sounds human, not robotic
- NO bullet points, NO numbered lists, NO "Key takeaway" labels
- Just a flowing 2-3 sentence answer

Example good response: "Manhattan had the most crimes reported with 147,832 incidents, followed closely by Brooklyn at 132,451. Interestingly, Staten Island had dramatically fewer — just 28,103 — which tracks with its smaller population."

Example BAD response (don't do this): "1. A total of 579,573 crimes were reported. 2. - The data reflects..."

Now write the answer:"""
    resp = client.responses.create(model=OPENAI_MODEL, input=prompt, temperature=0.3)
    return resp.output_text


def build_sql_prompt(question: str) -> str:
    # [PASTE YOUR EXISTING build_sql_prompt FUNCTION BODY HERE — UNCHANGED]
    # I'm keeping this short for space; your existing version is fine as-is
    prompt = f"""You are a SQL generator. Produce exactly one SQL SELECT statement (no surrounding text).

=== TABLE SCHEMAS ===

1. new_york_crime (enriched with ZIP codes via spatial join)
   Columns: CMPLNT_NUM, ADDR_PCT_CD, BORO_NM, CMPLNT_FR_DT, CMPLNT_FR_TM,
            CMPLNT_TO_DT, CMPLNT_TO_TM, CRM_ATPT_CPTD_CD, HADEVELOPT,
            HOUSING_PSA, JURISDICTION_CODE, JURIS_DESC, KY_CD, LAW_CAT_CD,
            LOC_OF_OCCUR_DESC, OFNS_DESC, PARKS_NM, PATROL_BORO, PD_CD,
            PD_DESC, PREM_TYP_DESC, RPT_DT, STATION_NAME, SUSP_AGE_GROUP,
            SUSP_RACE, SUSP_SEX, TRANSIT_DISTRICT, VIC_AGE_GROUP, VIC_RACE,
            VIC_SEX, X_COORD_CD, Y_COORD_CD, Latitude, Longitude, Lat_Lon, MODZCTA
   Geographic keys:
     - BORO_NM (borough, e.g. 'MANHATTAN', 'BROOKLYN', 'QUEENS', 'BRONX', 'STATEN ISLAND')
     - MODZCTA (ZIP code as integer, e.g. 10001) — use this to join to SVI, pop_zip, grocery, bus

2. new_york_svi (Social Vulnerability Index — one row per ZIP/ZCTA)
   Columns: FIPS, LOCATION, E_TOTPOP, RPL_THEMES, RPL_THEME1, RPL_THEME2,
            RPL_THEME3, RPL_THEME4, EP_POV150, EP_UNEMP, EP_HBURD, EP_NOHSDP,
            EP_UNINSUR, EP_AGE65, EP_AGE17, EP_DISABL, EP_LIMENG, EP_MINRTY,
            EP_MUNIT, EP_MOBILE, EP_CROWD, EP_NOVEH, EP_GROUPQ
   Geographic keys: FIPS (ZIP/ZCTA code as integer, e.g. 10001, 10002)
   RPL_THEMES = overall vulnerability (0 to 1, higher = more vulnerable).
   RPL_THEME1 = socioeconomic, RPL_THEME2 = household/disability,
   RPL_THEME3 = minority/language, RPL_THEME4 = housing/transportation.

3. new_york_pop_zip (Population by Modified ZIP Code)
   Columns: MODZCTA, label, ZCTA, pop_est
   Geographic keys: MODZCTA (ZIP code as integer, e.g. 10001, 10002)
   NOTE: pop_est is stored as a string with commas (e.g. '23,072'). Use:
         TRY_CAST(REPLACE(pop_est, ',', '') AS INT) to get numeric population.

4. new_york_grocery_coords (Grocery stores)
   Columns: store_dba, store_entity, zip_code, Georeference, Latitude, Longitude
   Geographic keys: zip_code (bigint, e.g. 10310)
   NOTE: zip_code is already an integer — NO backticks or TRY_CAST needed.
         store_dba = store display name, store_entity = legal entity name.

5. new_york_bus (enriched with ZIP codes via spatial join)
   Columns: BoroCode, BoroName, BoroCD, CounDist, AssemDist, StSenDist,
            CongDist, Shelter_ID, Corner, On_Street, Cross_Stre,
            Longitude, Latitude, NTAName, FEMAFldz, FEMAFldT, HrcEvac, MODZCTA
   Geographic keys:
     - BoroName (e.g. 'Manhattan', 'Brooklyn' — title case)
     - MODZCTA (ZIP code as integer) — use this to join to SVI, pop_zip, grocery, crime

=== JOIN KEYS (all tables connect via ZIP code) ===
- new_york_crime.MODZCTA = new_york_svi.FIPS
- new_york_crime.MODZCTA = new_york_pop_zip.MODZCTA
- new_york_crime.MODZCTA = new_york_grocery_coords.zip_code
- new_york_crime.MODZCTA = new_york_bus.MODZCTA
- new_york_bus.MODZCTA = new_york_svi.FIPS
- new_york_bus.MODZCTA = new_york_pop_zip.MODZCTA
- new_york_bus.MODZCTA = new_york_grocery_coords.zip_code
- new_york_svi.FIPS = new_york_pop_zip.MODZCTA
- new_york_svi.FIPS = new_york_grocery_coords.zip_code
- new_york_pop_zip.MODZCTA = new_york_grocery_coords.zip_code
- Borough-level: UPPER(new_york_bus.BoroName) = new_york_crime.BORO_NM

=== RULES ===
- Use ONLY columns listed above. Do NOT invent columns that don't exist.
- Crime type is OFNS_DESC. Crime severity is LAW_CAT_CD ('FELONY','MISDEMEANOR','VIOLATION').
- Do NOT use geom, the_geom, or Lat_Lon columns in queries — they are not useful for analysis.
- For property crime, use specific OFNS_DESC values like:
  'BURGLARY', 'GRAND LARCENY', 'PETIT LARCENY', 'GRAND LARCENY OF MOTOR VEHICLE',
  'ROBBERY', 'CRIMINAL MISCHIEF & RELATED OF', 'THEFT-FRAUD', 'STOLEN PROPERTY'.
  Do NOT use LIKE '%Property%'.
- Use RPL_THEMES as the overall vulnerability score. Use TRY_CAST(RPL_THEMES AS DOUBLE) if comparing.
- pop_est needs TRY_CAST(REPLACE(pop_est, ',', '') AS INT).
- Grocery zip_code is already a bigint — just use it directly, no casting needed.
- Grocery store name is store_dba. Legal entity name is store_entity.
- If counting, use COUNT(*) and always GROUP BY the grouped column.
- When calculating crime rates, divide crime count by population (from pop_zip).
- For safety/lighting questions, prioritize ZIP codes with high crime + high vulnerability.
- For transit accessibility, count bus stops per ZIP and cross with grocery store density.
- Output ONLY a single SELECT statement. No explanation, no markdown.

User question: "{question}"
Now output only the SELECT statement:
"""
    return prompt


def extract_first_select(full_text: str) -> str:
    if not full_text:
        return ""
    cleaned = re.sub(r"^```(?:sql)?\s*", "", full_text, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"\s*```$", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = cleaned.replace("\\n", " ").replace("\\t", " ").replace("\\r", " ")
    cleaned = cleaned.replace("\\", " ")
    m = re.search(r"(select\b.*)$", cleaned, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    candidate = m.group(1).strip()
    candidate = re.sub(r"\s+", " ", candidate).strip()
    candidate = re.sub(r"```$", "", candidate).strip()
    return candidate


@app.post("/ask")
async def ask(req: AskRequest):
    question = req.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Empty question")

    prompt = build_sql_prompt(question)

    try:
        resp = client.responses.create(model=OPENAI_MODEL, input=prompt, temperature=0.0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OpenAI error: {e}")

    full_text = getattr(resp, "output_text", "") or str(resp)
    sql_candidate = extract_first_select(full_text).strip()
    sql_candidate = sql_candidate.strip("`").strip('"').strip("'").strip().rstrip(";").strip()

    if not sql_candidate:
        raise HTTPException(status_code=400, detail=f"Model did not return SELECT: {full_text[:500]}")

    sql_clean = sanitize_sql(sql_candidate)
    print("=== SQL ===\n" + sql_clean)

    if "limit" not in sql_clean.lower() and req.max_rows:
        sql_clean = f"{sql_clean} LIMIT {int(req.max_rows)}"

    max_retries = 5
    for attempt in range(max_retries):
        try:
            rows = run_sql(sql_clean)
            break
        except Exception as e:
            error_msg = str(e)
            if "MISSING_AGGREGATION" in error_msg and attempt < max_retries - 1:
                match = re.search(r'"(\w+)".*is based on columns', error_msg)
                if match:
                    bad_col = match.group(1)
                    col_ref_match = re.search(r'(\w+\.)' + re.escape(bad_col), sql_clean)
                    full_ref = col_ref_match.group(0) if col_ref_match else bad_col
                    group_match = re.search(r'(GROUP\s+BY\s+)(.*?)(\s+HAVING|\s+ORDER|\s+LIMIT|$)', sql_clean, re.IGNORECASE)
                    if group_match and full_ref not in group_match.group(2):
                        sql_clean = sql_clean[:group_match.end(2)] + ', ' + full_ref + sql_clean[group_match.end(2):]
                        continue
                    raise HTTPException(status_code=500, detail=f"Databricks error: {e}")
                else:
                    raise HTTPException(status_code=500, detail=f"Databricks error: {e}")
            else:
                raise HTTPException(status_code=500, detail=f"Databricks error: {e}")

    answer = build_human_answer(question, sql_clean, rows)
    return {"sql": sql_clean, "rows": rows, "answer": answer}
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

default_args = {
    "owner"          : "inter24",
    "depends_on_past": False,
    "retries"        : 1,
    "retry_delay"    : timedelta(minutes=5),
}

@dag(
    dag_id      = "energy_economy_etl_int24",
    description = "ETL pipeline — U.S. Electricity & Global Economic Indicators 2022-2024",
    default_args= default_args,
    schedule    = None,
    start_date  = datetime(2022, 1, 1),
    catchup     = False,
    tags        = ["etl", "inter24", "energy", "economics"],
)
def energy_economy_pipeline():

    # ══════════════════════════════════════
    # EXTRACT 1 — EIA Retail
    # ══════════════════════════════════════
    @task()
    def extract_eia() -> dict:
        import requests, os

        API_KEY = os.getenv("EIA_API_KEY", "")
        url     = "https://api.eia.gov/v2/electricity/retail-sales/data"
        params  = {
            "api_key"            : API_KEY,
            "frequency"          : "monthly",
            "data[0]"            : "sales",
            "data[1]"            : "price",
            "data[2]"            : "revenue",
            "facets[stateid][]"  : "US",
            "facets[sectorid][]" : ["RES", "COM", "IND", "ALL"],
            "start"              : "2022-01",
            "end"                : "2024-12",
            "length"             : 5000,
        }
        resp    = requests.get(url, params=params)
        records = resp.json().get("response", {}).get("data", [])
        print(f"EIA retail: {len(records)} records")
        return {"records": records}

    # ══════════════════════════════════════
    # EXTRACT 2 — EIA Generation
    # ══════════════════════════════════════
    @task()
    def extract_eia_generation() -> dict:
        import requests, os

        API_KEY = os.getenv("EIA_API_KEY", "")
        url     = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data"
        params  = {
            "api_key"              : API_KEY,
            "frequency"            : "monthly",
            "data[0]"              : "generation",
            "facets[location][]"   : "US",
            "facets[fueltypeid][]" : ["COL", "NG", "NUC", "WND", "SUN", "HYC"],
            "start"                : "2022-01",
            "end"                  : "2024-12",
            "length"               : 5000,
        }
        resp    = requests.get(url, params=params)
        records = resp.json().get("response", {}).get("data", [])
        print(f"EIA generation: {len(records)} records")
        return {"records": records}

    # ══════════════════════════════════════
    # EXTRACT 3 — World Bank CSV
    # ══════════════════════════════════════
    @task()
    def extract_worldbank() -> dict:
        import pandas as pd, os

        raw_dir = "/home/inter24/dags/energy_dwh/raw/"
        files   = {
            "wb_cpi.csv"          : "cpi_pct_yoy",
            "wb_gdp.csv"          : "gdp_current_usd_mn",
            "wb_industrial.csv"   : "industrial_prod_usd",
            "wb_unemployment.csv" : "unemployment_rate",
            "wb_exchange_rate.csv": "exchange_rate_lcu_usd",
        }

        result = {}
        for fname, indicator in files.items():
            fpath = os.path.join(raw_dir, fname)
            if os.path.exists(fpath):
                df = pd.read_csv(fpath)
                result[indicator] = df.to_dict(orient="records")
                print(f"{fname}: {len(df)} rows")
            else:
                print(f"✗ Tidak ditemukan: {fpath}")
                result[indicator] = []

        return result

    # ══════════════════════════════════════
    # TRANSFORM
    # ══════════════════════════════════════
    @task()
    def transform(eia_retail: dict, eia_gen: dict, wb_data: dict) -> dict:
        import pandas as pd

        print("=" * 50)
        print("TRANSFORM — mulai")
        print("=" * 50)

        # ── Helper: normalize period ──────────
        def normalize_period(p):
            p = str(p).strip()
            if "M" in p:
                parts = p.split("M")
                return f"{parts[0]}-{parts[1].zfill(2)}"
            if len(p) == 7 and "-" in p:
                return p
            return None

        # ── A. Transform EIA Retail ───────────
        df_retail = pd.DataFrame(eia_retail["records"])
        print(f"EIA retail raw: {df_retail.shape}")

        if not df_retail.empty:
            df_retail.columns = [c.lower().strip() for c in df_retail.columns]
            cols_keep = ["period", "stateid", "sectorid", "sectorname",
                         "sales", "price", "revenue"]
            available = [c for c in cols_keep if c in df_retail.columns]
            df_retail = df_retail[available]

            df_retail = df_retail.rename(columns={
                "stateid"   : "state_id",
                "sectorid"  : "sector_id",
                "sectorname": "sector_name",
                "sales"     : "retail_sales_mwh",
                "price"     : "price_cents_kwh",
                "revenue"   : "revenue_million_usd",
            })

            df_retail = df_retail[df_retail["state_id"] == "US"].copy()

            for col in ["retail_sales_mwh", "price_cents_kwh", "revenue_million_usd"]:
                if col in df_retail.columns:
                    df_retail[col] = pd.to_numeric(df_retail[col], errors="coerce")

            df_retail = df_retail.dropna(
                subset=["price_cents_kwh", "retail_sales_mwh"], how="all"
            )
            df_retail = df_retail.drop_duplicates()

            df_retail["period_dt"] = pd.to_datetime(
                df_retail["period"], format="%Y-%m", errors="coerce"
            )
            df_retail["year"]    = df_retail["period_dt"].dt.year
            df_retail["month"]   = df_retail["period_dt"].dt.month
            df_retail["quarter"] = df_retail["period_dt"].dt.quarter

            print(f"EIA retail clean: {df_retail.shape}")

        # ── B. Transform EIA Generation ───────
        df_gen = pd.DataFrame(eia_gen["records"])
        print(f"EIA generation raw: {df_gen.shape}")

        if not df_gen.empty:
            df_gen.columns = [c.lower().strip() for c in df_gen.columns]

            rename_map = {}
            for col in df_gen.columns:
                if "fuel" in col and "type" in col and "id" in col:
                    rename_map[col] = "fuel_type_id"
                elif col == "generation":
                    rename_map[col] = "net_generation_mwh"
                elif col == "location":
                    rename_map[col] = "location_id"
            df_gen = df_gen.rename(columns=rename_map)

            df_gen["net_generation_mwh"] = pd.to_numeric(
                df_gen["net_generation_mwh"], errors="coerce"
            )
            df_gen = df_gen.dropna(subset=["net_generation_mwh"])
            df_gen = df_gen[df_gen["net_generation_mwh"] >= 0]

            fuel_mapping = {
                "COL": "Coal",        "NG" : "Natural Gas",
                "NUC": "Nuclear",     "WND": "Wind",
                "SUN": "Solar",       "HYC": "Hydroelectric",
                "GEO": "Geothermal",  "OTH": "Other",
            }
            fossil    = ["COL", "NG", "PET", "OOG"]
            renewable = ["WND", "SUN", "HYC", "GEO", "WAS", "BIO"]

            if "fuel_type_id" in df_gen.columns:
                df_gen["fuel_name"]     = df_gen["fuel_type_id"].map(fuel_mapping).fillna("Other")
                df_gen["fuel_category"] = df_gen["fuel_type_id"].apply(
                    lambda x: "Fossil" if x in fossil
                    else "Renewable" if x in renewable
                    else "Nuclear" if x == "NUC" else "Other"
                )
                df_gen["is_renewable"] = df_gen["fuel_type_id"].isin(renewable)

            df_gen["period_dt"] = pd.to_datetime(
                df_gen["period"], format="%Y-%m", errors="coerce"
            )
            df_gen["year"]    = df_gen["period_dt"].dt.year
            df_gen["month"]   = df_gen["period_dt"].dt.month
            df_gen["quarter"] = df_gen["period_dt"].dt.quarter

            df_gen = df_gen.drop_duplicates()
            print(f"EIA generation clean: {df_gen.shape}")

        # ── C. Transform World Bank ───────────
        wb_frames = {}
        for indicator, records in wb_data.items():
            if not records:
                print(f"{indicator}: kosong")
                continue

            df_wb = pd.DataFrame(records)
            df_wb["period"] = df_wb["period_raw"].apply(normalize_period)
            df_wb = df_wb.dropna(subset=["period"])
            df_wb[indicator] = pd.to_numeric(df_wb[indicator], errors="coerce")
            df_wb = df_wb.dropna(subset=[indicator])
            df_wb = df_wb[
                df_wb["period"].str.startswith(("2022", "2023", "2024"))
            ]
            df_wb = df_wb.drop_duplicates(subset=["period", "country"])
            wb_frames[indicator] = df_wb[["period", "country", indicator]]
            print(f"{indicator}: {df_wb.shape}")

        # ── D. Gabungkan World Bank ───────────
        if wb_frames:
            indicators = list(wb_frames.keys())
            df_wb_all  = wb_frames[indicators[0]]
            for ind in indicators[1:]:
                df_wb_all = df_wb_all.merge(
                    wb_frames[ind], on=["period", "country"], how="outer"
                )
            print(f"World Bank combined: {df_wb_all.shape}")
        else:
            df_wb_all = pd.DataFrame()

        # ── E. Summary ────────────────────────
        summary = {
            "retail_rows"    : len(df_retail) if not df_retail.empty else 0,
            "generation_rows": len(df_gen)    if not df_gen.empty    else 0,
            "wb_rows"        : len(df_wb_all) if not df_wb_all.empty else 0,
            "wb_countries"   : df_wb_all["country"].nunique() if not df_wb_all.empty else 0,
            "status"         : "OK",
        }

        print("\n" + "=" * 50)
        print(f"TRANSFORM selesai: {summary}")
        print("=" * 50)

        return {
            "retail"    : df_retail.to_dict(orient="records")   if not df_retail.empty else [],
            "generation": df_gen.to_dict(orient="records")      if not df_gen.empty    else [],
            "worldbank" : df_wb_all.to_dict(orient="records")   if not df_wb_all.empty else [],
            "summary"   : summary,
        }

    # ══════════════════════════════════════
    # LOAD — Supabase
    # ══════════════════════════════════════
    @task()
    def load(transform_result: dict) -> None:
        import pandas as pd
        import os
        from sqlalchemy import create_engine, text

        print("=" * 50)
        print("LOAD — mulai")
        print("=" * 50)

        # Ambil connection string dari environment
        DB_URL = os.getenv("SUPABASE_DB_URL", "")
        if not DB_URL:
            print("SUPABASE_DB_URL tidak ditemukan di environment")
            print("Set dengan: export SUPABASE_DB_URL='postgresql://...'")
            return

        engine = create_engine(DB_URL)

        # ── Load Retail ───────────────────────
        retail_records = transform_result.get("retail", [])
        if retail_records:
            df_retail = pd.DataFrame(retail_records)
            # Pilih kolom yang ada di tabel Supabase
            cols = ["period", "year", "sector_id", "price_cents_kwh",
                    "retail_sales_mwh", "revenue_million_usd"]
            cols_available = [c for c in cols if c in df_retail.columns]
            df_retail[cols_available].to_sql(
                "fact_energy_economy",
                engine,
                if_exists="append",
                index=False,
                chunksize=500,
            )
            print(f"fact_energy_economy: {len(df_retail)} rows di-load")

        # ── Load Generation ───────────────────
        gen_records = transform_result.get("generation", [])
        if gen_records:
            df_gen = pd.DataFrame(gen_records)
            cols = ["period", "year", "month", "quarter", "location_id",
                    "fuel_type_id", "fuel_name", "fuel_category",
                    "is_renewable", "net_generation_mwh"]
            cols_available = [c for c in cols if c in df_gen.columns]
            df_gen[cols_available].to_sql(
                "fact_generation",
                engine,
                if_exists="append",
                index=False,
                chunksize=500,
            )
            print(f"fact_generation: {len(df_gen)} rows di-load")

        # ── Refresh Materialized Views ─────────
        with engine.connect() as conn:
            try:
                conn.execute(text(
                    "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_generation_monthly"
                ))
                conn.execute(text(
                    "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_retail_economic"
                ))
                conn.commit()
                print("Materialized Views di-refresh")
            except Exception as e:
                print(f"Refresh MV gagal (normal jika data pertama kali): {e}")

        print("=" * 50)
        print("LOAD selesai ✓")
        print("=" * 50)

    # ══════════════════════════════════════
    # NOTIFY
    # ══════════════════════════════════════
    notify = BashOperator(
        task_id     = "notify_completion",
        bash_command= 'echo "Pipeline energy_economy_etl_int24 selesai: $(date)"',
    )

    # ── Task dependencies ──────────────────
    retail = extract_eia()
    gen    = extract_eia_generation()
    wb     = extract_worldbank()
    result = transform(retail, gen, wb)
    load(result) >> notify

energy_economy_pipeline()
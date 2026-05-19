from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# ── Konstanta path — di luar @dag agar selalu tersedia di worker
STAGING  = "/home/inter24/energy_dwh/staging"
RAW_DIR  = "/home/inter24/energy_dwh/raw"

default_args = {
    "owner"          : "inter24",
    "depends_on_past": False,
    "retries"        : 1,
    "retry_delay"    : timedelta(minutes=5),
}

@dag(
    dag_id      = "energy_economy_etl_int24",
    description = "ETL pipeline - U.S. Electricity & Global Economic Indicators 2022-2024",
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
    def extract_eia() -> str:
        import requests, os, pandas as pd

        API_KEY = os.getenv("EIA_API_KEY", "")
        os.makedirs(STAGING, exist_ok=True)

        url    = "  "
        params = {
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

        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        records = resp.json().get("response", {}).get("data", [])

        out = f"{STAGING}/eia_retail.csv"
        pd.DataFrame(records).to_csv(out, index=False)
        print(f"EIA retail: {len(records)} records → {out}")
        return out

    # ══════════════════════════════════════
    # EXTRACT 2 — EIA Generation
    # ══════════════════════════════════════
    @task()
    def extract_eia_generation() -> str:
        import requests, os, pandas as pd

        API_KEY = os.getenv("EIA_API_KEY", "")
        os.makedirs(STAGING, exist_ok=True)

        url    = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data"
        params = {
            "api_key"              : API_KEY,
            "frequency"            : "monthly",
            "data[0]"              : "generation",
            "facets[location][]"   : "US",
            "facets[fueltypeid][]" : ["COL", "NG", "NUC", "WND", "SUN", "HYC"],
            "start"                : "2022-01",
            "end"                  : "2024-12",
            "length"               : 5000,
        }

        # BUG 5 FIX: raise_for_status + timeout
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        records = resp.json().get("response", {}).get("data", [])

        out = f"{STAGING}/eia_generation.csv"
        pd.DataFrame(records).to_csv(out, index=False)
        print(f"EIA generation: {len(records)} records → {out}")
        return out

    # ══════════════════════════════════════
    # EXTRACT 3 — World Bank CSV
    # ══════════════════════════════════════
    @task()
    def extract_worldbank() -> str:
        import pandas as pd, os
        from functools import reduce

        os.makedirs(STAGING, exist_ok=True)

        files = {
            "wb_cpi.csv"          : "cpi_pct_yoy",
            "wb_gdp.csv"          : "gdp_current_usd_mn",
            "wb_industrial.csv"   : "industrial_prod_usd",
            "wb_unemployment.csv" : "unemployment_rate",
            "wb_exchange_rate.csv": "exchange_rate_lcu_usd",
        }

        dfs = []
        for fname, indicator in files.items():
            fpath = os.path.join(RAW_DIR, fname)
            if os.path.exists(fpath):
                df = pd.read_csv(fpath)
                print(f"{fname}: {df.shape} | kolom: {df.columns.tolist()}")
                dfs.append(df)
            else:
                print(f"Tidak ditemukan: {fpath}")

        if dfs:
            df_all = reduce(
                lambda l, r: pd.merge(l, r, on=["period_raw", "country"], how="outer"),
                dfs
            )
            out = f"{STAGING}/wb_combined.csv"
            df_all.to_csv(out, index=False)
            print(f"World Bank combined: {df_all.shape} → {out}")
            return out
        else:
            print("Tidak ada file World Bank ditemukan")
            return ""

    # ══════════════════════════════════════
    # TRANSFORM
    # ══════════════════════════════════════
    @task()
    def transform(retail_path: str, gen_path: str, wb_path: str) -> dict:
        import pandas as pd
        import os

        print("=" * 50)
        print("TRANSFORM — mulai")
        print("=" * 50)

        clean_dir = f"{STAGING}/clean"
        os.makedirs(clean_dir, exist_ok=True)

        def normalize_period(p):
            p = str(p).strip()
            if "M" in p:
                parts = p.split("M")
                return f"{parts[0]}-{parts[1].zfill(2)}"
            if len(p) == 7 and "-" in p:
                return p
            return None

        df_retail = pd.DataFrame()
        df_gen    = pd.DataFrame()

        # ── A. Transform EIA Retail ───────────
        print(f"\nMembaca: {retail_path}")
        df_retail = pd.read_csv(retail_path)
        print(f"EIA retail raw: {df_retail.shape} | kolom: {df_retail.columns.tolist()}")

        if not df_retail.empty:
            df_retail.columns = [c.lower().strip() for c in df_retail.columns]
            rename_map = {
                "stateid"   : "state_id",
                "sectorid"  : "sector_id",
                "sectorname": "sector_name",
                "sales"     : "retail_sales_mwh",
                "price"     : "price_cents_kwh",
                "revenue"   : "revenue_million_usd",
            }
            df_retail = df_retail.rename(columns=rename_map)
            if "state_id" in df_retail.columns:
                df_retail = df_retail[df_retail["state_id"] == "US"].copy()
            for col in ["retail_sales_mwh", "price_cents_kwh", "revenue_million_usd"]:
                if col in df_retail.columns:
                    df_retail[col] = pd.to_numeric(df_retail[col], errors="coerce")
            df_retail["period_dt"] = pd.to_datetime(df_retail["period"], format="%Y-%m", errors="coerce")
            df_retail["year"]    = df_retail["period_dt"].dt.year
            df_retail["month"]   = df_retail["period_dt"].dt.month
            df_retail["quarter"] = df_retail["period_dt"].dt.quarter
            df_retail = df_retail.drop_duplicates().reset_index(drop=True)

            out_retail = f"{clean_dir}/fact_retail.csv"
            df_retail.to_csv(out_retail, index=False)
            print(f"EIA retail clean: {df_retail.shape} → {out_retail}")

        # ── B. Transform EIA Generation ───────
        print(f"\nMembaca: {gen_path}")
        df_gen = pd.read_csv(gen_path)
        print(f"EIA generation raw: {df_gen.shape} | kolom: {df_gen.columns.tolist()}")

        if not df_gen.empty:
            df_gen.columns = [c.lower().strip() for c in df_gen.columns]
            fuel_col = next((c for c in df_gen.columns if "fuel" in c and "id" in c), None)
            if fuel_col and fuel_col != "fuel_type_id":
                df_gen = df_gen.rename(columns={fuel_col: "fuel_type_id"})
            df_gen = df_gen.rename(columns={"generation": "net_generation_mwh"})
            df_gen["net_generation_mwh"] = pd.to_numeric(df_gen["net_generation_mwh"], errors="coerce")
            df_gen = df_gen.dropna(subset=["net_generation_mwh"])
            df_gen = df_gen[df_gen["net_generation_mwh"] >= 0]

            fuel_map  = {"COL": "Coal", "NG": "Natural Gas", "NUC": "Nuclear",
                         "WND": "Wind", "SUN": "Solar", "HYC": "Hydroelectric",
                         "GEO": "Geothermal", "OTH": "Other"}
            renewable = ["WND", "SUN", "HYC", "GEO", "WAS", "BIO"]
            fossil    = ["COL", "NG", "PET", "OOG"]

            if "fuel_type_id" in df_gen.columns:
                df_gen["fuel_name"]     = df_gen["fuel_type_id"].map(fuel_map).fillna("Other")
                df_gen["fuel_category"] = df_gen["fuel_type_id"].apply(
                    lambda x: "Fossil" if x in fossil
                    else "Renewable" if x in renewable
                    else "Nuclear" if x == "NUC" else "Other"
                )
                df_gen["is_renewable"] = df_gen["fuel_type_id"].isin(renewable)

            df_gen["period_dt"] = pd.to_datetime(df_gen["period"], format="%Y-%m", errors="coerce")
            df_gen["year"]    = df_gen["period_dt"].dt.year
            df_gen["month"]   = df_gen["period_dt"].dt.month
            df_gen["quarter"] = df_gen["period_dt"].dt.quarter
            df_gen = df_gen.drop_duplicates().reset_index(drop=True)

            out_gen = f"{clean_dir}/fact_generation.csv"
            df_gen.to_csv(out_gen, index=False)
            print(f"EIA generation clean: {df_gen.shape} → {out_gen}")

        # ── C. Transform World Bank ───────────
        if wb_path and os.path.exists(wb_path):
            print(f"\nMembaca: {wb_path}")
            df_wb = pd.read_csv(wb_path)
            df_wb["period"] = df_wb["period_raw"].apply(normalize_period)
            df_wb = df_wb.dropna(subset=["period"])
            df_wb = df_wb[df_wb["period"].str.startswith(("2022", "2023", "2024"))]
            indicator_cols = [c for c in df_wb.columns if c not in ["period_raw", "period", "country"]]
            for col in indicator_cols:
                df_wb[col] = pd.to_numeric(df_wb[col], errors="coerce")
            df_wb["period_dt"] = pd.to_datetime(df_wb["period"], format="%Y-%m", errors="coerce")
            df_wb["year"]    = df_wb["period_dt"].dt.year
            df_wb["month"]   = df_wb["period_dt"].dt.month
            df_wb["quarter"] = df_wb["period_dt"].dt.quarter
            df_wb = df_wb.drop_duplicates(subset=["period", "country"]).reset_index(drop=True)
            out_wb = f"{clean_dir}/worldbank_combined.csv"
            df_wb.to_csv(out_wb, index=False)
            print(f"World Bank clean: {df_wb.shape} → {out_wb}")
        else:
            print("World Bank file tidak tersedia")

        # BUG 2 FIX: df_retail dan df_gen sudah pasti terdefinisi karena inisialisasi di atas
        summary = {
            "retail_rows"    : len(df_retail),
            "generation_rows": len(df_gen),
            "status"         : "OK",
        }
        print(f"\nTRANSFORM selesai: {summary}")
        return summary

    # ══════════════════════════════════════
    # LOAD — Supabase
    # ══════════════════════════════════════
    @task()
    def load(summary: dict) -> None:
        import pandas as pd, os
        from sqlalchemy import create_engine, text

        print("=" * 50)
        print("LOAD — mulai")
        print(f"Summary dari transform: {summary}")

        DB_URL = os.getenv("SUPABASE_DB_URL", "")
        if not DB_URL:
            print("SUPABASE_DB_URL tidak ditemukan — skip load")
            return

        engine    = create_engine(DB_URL)
        clean_dir = f"{STAGING}/clean"

        retail_path = f"{clean_dir}/fact_retail.csv"
        if os.path.exists(retail_path):
            df   = pd.read_csv(retail_path)
            cols = ["period", "year", "sector_id", "price_cents_kwh",
                    "retail_sales_mwh", "revenue_million_usd"]
            cols_ok = [c for c in cols if c in df.columns]
            df[cols_ok].to_sql("fact_energy_economy", engine,
                               if_exists="append", index=False, chunksize=500)
            print(f"fact_energy_economy: {len(df)} rows")

        gen_path = f"{clean_dir}/fact_generation.csv"
        if os.path.exists(gen_path):
            df   = pd.read_csv(gen_path)
            cols = ["period", "year", "month", "quarter",
                    "fuel_type_id", "fuel_name", "fuel_category",
                    "is_renewable", "net_generation_mwh"]
            cols_ok = [c for c in cols if c in df.columns]
            df[cols_ok].to_sql("fact_generation", engine,
                               if_exists="append", index=False, chunksize=500)
            print(f"fact_generation: {len(df)} rows")

        with engine.connect() as conn:
            try:
                conn.execute(text("REFRESH MATERIALIZED VIEW mv_generation_monthly"))
                conn.execute(text("REFRESH MATERIALIZED VIEW mv_retail_economic"))
                conn.commit()
                print("Materialized Views di-refresh")
            except Exception as e:
                print(f"Refresh MV: {e}")

    # ══════════════════════════════════════
    # NOTIFY
    # ══════════════════════════════════════
    notify = BashOperator(
        task_id     = "notify_completion",
        bash_command= 'echo "Pipeline energy_economy_etl_int24 selesai: $(date)"',
    )

    retail = extract_eia()
    gen    = extract_eia_generation()
    wb     = extract_worldbank()
    result = transform(retail, gen, wb)
    load(result) >> notify

energy_economy_pipeline()
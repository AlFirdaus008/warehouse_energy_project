# INT24 S1 Sains Data FMIPA UNESA 2025/2026
# Tim: Abdullah Al-Firdaus Nuzula, Mylovia Mahesa Ayu,
#      Fio Ulaa' Octriyanti, Muhammad Raffi Fahrezi
# Schedule : setiap hari jam 12 siang (UTC+7 = 05:00 UTC)
# Server   : airflow.icaiunesa.dev  |  user: inter24
# DAG path : /home/inter24/dags/energy_etl_dag.py

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv("/home/inter24/dags/energy_dwh/.env")

import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine, text

from airflow.decorators import dag, task

# Konfigurasi Path
BASE_DIR    = Path("/home/inter24/dags/energy_dwh")
RAW_DIR     = BASE_DIR / "raw"
STAGING_DIR = Path("/tmp/energy_dwh/staging")
CLEAN_DIR   = Path("/tmp/energy_dwh/clean")

# Konstanta Dataset
EIA_BASE_URL = "https://api.eia.gov/v2"
TARGET_COUNTRIES = [
    "United States", "China", "Germany", "Japan",
    "India", "United Kingdom", "France", "Brazil"
]
COUNTRY_CODE_MAP = {
    "United States": "US", "China": "CN", "Germany": "DE",
    "Japan": "JP",         "India": "IN", "United Kingdom": "GB",
    "France": "FR",        "Brazil": "BR"
}
FUEL_CATEGORY_MAP = {
    "COL": ("Coal",          "Fossil",    False),
    "NG":  ("Natural Gas",   "Fossil",    False),
    "NUC": ("Nuclear",       "Nuclear",   False),
    "WND": ("Wind",          "Renewable", True),
    "SUN": ("Solar",         "Renewable", True),
    "HYC": ("Hydroelectric", "Renewable", True),
    "GEO": ("Geothermal",    "Renewable", True),
    "OTH": ("Other",         "Other",     False),
}

log = logging.getLogger(__name__)


@dag(
    dag_id="energy_economy_etl",
    description=(
        "ETL Pipeline: EIA Electricity API + World Bank CSV, "
        "Transform Star Schema, Load Supabase PostgreSQL"
    ),
    schedule="0 5 * * *",
    start_date=datetime(2025, 5, 19),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "owner": "inter24",
    },
    tags=["energy", "worldbank", "eia", "supabase", "int24"],
)
def energy_economy_etl():

    # TASK 1A - EXTRACT: EIA Retail Sales & Price
    @task(task_id="extract_eia_retail")
    def extract_eia_retail() -> str:
        STAGING_DIR.mkdir(parents=True, exist_ok=True)

        api_key = os.environ.get("EIA_API_KEY", "")
        if not api_key:
            raise EnvironmentError("EIA_API_KEY tidak ditemukan di environment.")

        url = f"{EIA_BASE_URL}/electricity/retail-sales/data"
        params = {
            "api_key":             api_key,
            "frequency":           "monthly",
            "data[0]":             "sales",
            "data[1]":             "price",
            "data[2]":             "revenue",
            "facets[stateid][]":   "US",
            "facets[sectorid][]":  ["RES", "COM", "IND", "ALL"],
            "start":               "2022-01",
            "end":                 "2024-12",
            "sort[0][column]":     "period",
            "sort[0][direction]":  "asc",
            "offset":              0,
            "length":              5000,
        }

        all_records = []
        while True:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            body    = resp.json().get("response", {})
            records = body.get("data", [])
            total   = int(body.get("total", 0))

            if not records:
                log.warning("EIA Retail: tidak ada data.")
                break

            all_records.extend(records)
            log.info("EIA Retail: %d / %d records", len(all_records), total)

            if len(all_records) >= total:
                break
            params["offset"] += params["length"]

        df = pd.DataFrame(all_records)
        out_path = str(STAGING_DIR / "eia_retail_2022_2024.csv")
        df.to_csv(out_path, index=False)
        log.info("EIA Retail disimpan ke %s  shape=%s", out_path, df.shape)
        return out_path

    # TASK 1B - EXTRACT: EIA Net Generation by Fuel Type
    @task(task_id="extract_eia_generation")
    def extract_eia_generation() -> str:
        STAGING_DIR.mkdir(parents=True, exist_ok=True)

        api_key = os.environ.get("EIA_API_KEY", "")
        if not api_key:
            raise EnvironmentError("EIA_API_KEY tidak ditemukan di environment.")

        url = f"{EIA_BASE_URL}/electricity/electric-power-operational-data/data"
        params = {
            "api_key":               api_key,
            "frequency":             "monthly",
            "data[0]":               "generation",
            "facets[location][]":    "US",
            "facets[fueltypeid][]":  ["COL", "NG", "NUC", "WND",
                                      "SUN", "HYC", "GEO", "OTH"],
            "start":                 "2022-01",
            "end":                   "2024-12",
            "sort[0][column]":       "period",
            "sort[0][direction]":    "asc",
            "offset":                0,
            "length":                5000,
        }

        all_records = []
        while True:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            body    = resp.json().get("response", {})
            records = body.get("data", [])
            total   = int(body.get("total", 0))

            if not records:
                log.warning("EIA Generation: tidak ada data.")
                break

            all_records.extend(records)
            log.info("EIA Generation: %d / %d records", len(all_records), total)

            if len(all_records) >= total:
                break
            params["offset"] += params["length"]

        df = pd.DataFrame(all_records)
        out_path = str(STAGING_DIR / "eia_generation_2022_2024.csv")
        df.to_csv(out_path, index=False)
        log.info("EIA Generation disimpan ke %s  shape=%s", out_path, df.shape)
        return out_path

    # TASK 1C - EXTRACT: World Bank CSV
    @task(task_id="extract_worldbank")
    def extract_worldbank() -> str:
        STAGING_DIR.mkdir(parents=True, exist_ok=True)

        file_map = {
            "cpi":           "wb_cpi.csv",
            "gdp":           "wb_gdp.csv",
            "industrial":    "wb_industrial.csv",
            "unemployment":  "wb_unemployment.csv",
            "exchange_rate": "wb_exchange_rate.csv",
        }

        saved_files = []

        for indicator, filename in file_map.items():
            src = RAW_DIR / filename
            if not src.exists():
                raise FileNotFoundError(
                    f"File tidak ditemukan: {src}\n"
                    f"scp {filename} inter24@178.128.52.238:"
                    f"/home/inter24/dags/energy_dwh/raw/"
                )

            df_wide = pd.read_csv(src)
            df_wide.columns = [c.strip() for c in df_wide.columns]

            country_cols = [c for c in TARGET_COUNTRIES if c in df_wide.columns]
            df_wide = df_wide[["period_raw"] + country_cols].copy()

            df_long = df_wide.melt(
                id_vars="period_raw",
                value_vars=country_cols,
                var_name="country_name",
                value_name="value"
            )

            def parse_period(p):
                p = str(p).strip()
                if "M" in p:
                    parts = p.split("M")
                    return f"{parts[0]}-{parts[1].zfill(2)}"
                else:
                    year = int(float(p))
                    return f"{year}-01"

            df_long["period"]       = df_long["period_raw"].apply(parse_period)
            df_long = df_long[df_long["period"].between("2022-01", "2024-12")].copy()
            df_long["indicator"]    = indicator
            df_long["country_code"] = df_long["country_name"].map(COUNTRY_CODE_MAP)
            df_long["value"]        = pd.to_numeric(df_long["value"], errors="coerce")
            df_long = df_long.drop(columns=["period_raw"]).reset_index(drop=True)

            out_path = str(STAGING_DIR / f"wb_{indicator}_2022_2024.csv")
            df_long.to_csv(out_path, index=False)
            saved_files.append(out_path)
            log.info("WB %-15s disimpan ke %s  shape=%s",
                     indicator, out_path, df_long.shape)

        return ",".join(saved_files)

    # TASK 2 - TRANSFORM: Bentuk Star Schema
    @task(task_id="transform_all")
    def transform_all(retail_path: str, gen_path: str, wb_paths: str) -> str:
        CLEAN_DIR.mkdir(parents=True, exist_ok=True)

        df_retail = pd.read_csv(retail_path)
        df_gen    = pd.read_csv(gen_path)
        wb_files  = {p.split("wb_")[1].replace("_2022_2024.csv", ""): pd.read_csv(p)
                     for p in wb_paths.split(",") if p}

        # Transform EIA Retail
        df_retail.columns = [c.strip() for c in df_retail.columns]
        df_retail = df_retail.rename(columns={
            "stateid":    "state_id",
            "sectorid":   "sector_id",
            "sectorName": "sector_name",
            "sales":      "retail_sales_mwh",
            "price":      "price_cents_kwh",
            "revenue":    "revenue_million_usd",
        })
        if "state_id" in df_retail.columns:
            df_retail = df_retail[df_retail["state_id"] == "US"].copy()
        for col in ["retail_sales_mwh", "price_cents_kwh", "revenue_million_usd"]:
            if col in df_retail.columns:
                df_retail[col] = pd.to_numeric(df_retail[col], errors="coerce")
        df_retail["period"]     = pd.to_datetime(df_retail["period"], format="%Y-%m", errors="coerce")
        df_retail["year"]       = df_retail["period"].dt.year
        df_retail["month"]      = df_retail["period"].dt.month
        df_retail["quarter"]    = df_retail["period"].dt.quarter
        df_retail["period_str"] = df_retail["period"].dt.strftime("%Y-%m")
        df_retail = df_retail.dropna(subset=["period"]).drop_duplicates()

        # Transform EIA Generation
        df_gen.columns = [c.strip() for c in df_gen.columns]
        df_gen = df_gen.rename(columns={
            "location":            "location_id",
            "sectorid":            "sector_id",
            "fueltypeid":          "fuel_type_id",
            "fuelTypeDescription": "fuel_description",
            "generation":          "net_generation_mwh",
        })
        if "sector_id" in df_gen.columns:
            df_gen["sector_id"] = pd.to_numeric(df_gen["sector_id"], errors="coerce")
            df_gen = df_gen[df_gen["sector_id"] == 1].copy()
        df_gen["net_generation_mwh"] = pd.to_numeric(df_gen["net_generation_mwh"], errors="coerce")
        df_gen["fuel_name"]     = df_gen["fuel_type_id"].map(lambda x: FUEL_CATEGORY_MAP.get(x, ("Other", "Other", False))[0])
        df_gen["fuel_category"] = df_gen["fuel_type_id"].map(lambda x: FUEL_CATEGORY_MAP.get(x, ("Other", "Other", False))[1])
        df_gen["is_renewable"]  = df_gen["fuel_type_id"].map(lambda x: FUEL_CATEGORY_MAP.get(x, ("Other", "Other", False))[2])
        df_gen["period"]     = pd.to_datetime(df_gen["period"], format="%Y-%m", errors="coerce")
        df_gen["year"]       = df_gen["period"].dt.year
        df_gen["month"]      = df_gen["period"].dt.month
        df_gen["quarter"]    = df_gen["period"].dt.quarter
        df_gen["period_str"] = df_gen["period"].dt.strftime("%Y-%m")
        df_gen = df_gen.dropna(subset=["period"]).drop_duplicates()

        # Transform World Bank
        wb_pivoted = {}
        for indicator, df_wb in wb_files.items():
            df_wb["value"] = pd.to_numeric(df_wb["value"], errors="coerce")
            wb_pivoted[indicator] = df_wb.rename(columns={"value": indicator})

        if "cpi" in wb_pivoted:
            df_wb_merged = wb_pivoted["cpi"][["period", "country_name", "country_code", "cpi"]].copy()
            for ind in ["gdp", "industrial", "unemployment", "exchange_rate"]:
                if ind in wb_pivoted:
                    df_wb_merged = df_wb_merged.merge(
                        wb_pivoted[ind][["period", "country_code", ind]],
                        on=["period", "country_code"], how="left"
                    )
            if "gdp" in df_wb_merged.columns:
                df_wb_merged = df_wb_merged.sort_values(["country_code", "period"])
                df_wb_merged["gdp"] = df_wb_merged.groupby("country_code")["gdp"].ffill()
        else:
            first_key = list(wb_pivoted.keys())[0]
            df_wb_merged = wb_pivoted[first_key][["period", "country_name", "country_code"]].drop_duplicates()

        # dim_time
        all_periods = pd.date_range("2022-01-01", "2024-12-01", freq="MS")
        dim_time = pd.DataFrame({
            "time_id":    range(1, len(all_periods) + 1),
            "period":     all_periods.strftime("%Y-%m"),
            "year":       all_periods.year,
            "month":      all_periods.month,
            "quarter":    all_periods.quarter,
            "month_name": all_periods.strftime("%B"),
        })

        # dim_sector
        dim_sector = pd.DataFrame([
            {"sector_id": "ALL", "sector_name": "All Sectors"},
            {"sector_id": "RES", "sector_name": "Residential"},
            {"sector_id": "COM", "sector_name": "Commercial"},
            {"sector_id": "IND", "sector_name": "Industrial"},
        ])

        # dim_fuel_type
        dim_fuel_type = pd.DataFrame([
            {"fuel_id": i + 1, "fuel_type_id": k, "fuel_name": v[0],
             "fuel_category": v[1], "is_renewable": v[2]}
            for i, (k, v) in enumerate(FUEL_CATEGORY_MAP.items())
        ])

        # dim_country
        region_map = {
            "US": "North America", "CN": "Asia Pacific", "DE": "Europe",
            "JP": "Asia Pacific",  "IN": "Asia Pacific", "GB": "Europe",
            "FR": "Europe",        "BR": "South America",
        }
        income_map = {
            "US": "High", "CN": "Upper-Middle", "DE": "High",
            "JP": "High", "IN": "Lower-Middle",  "GB": "High",
            "FR": "High", "BR": "Upper-Middle",
        }
        dim_country = pd.DataFrame([
            {"country_id": i + 1, "country_code": code, "country_name": name,
             "region": region_map.get(code, "Unknown"), "income_group": income_map.get(code, "Unknown")}
            for i, (name, code) in enumerate(COUNTRY_CODE_MAP.items())
        ])

        # fact_energy_economy
        df_eia_us = df_retail[["period_str", "sector_id", "price_cents_kwh",
                                "retail_sales_mwh", "revenue_million_usd", "year"]].copy()
        df_eia_us["country_code"] = "US"
        df_wb_us = df_wb_merged[df_wb_merged["country_code"] == "US"].copy()
        df_wb_us = df_wb_us.rename(columns={
            "cpi":           "cpi_pct_yoy",
            "gdp":           "gdp_current_usd_mn",
            "industrial":    "industrial_prod_usd",
            "unemployment":  "unemployment_rate",
            "exchange_rate": "exchange_rate_lcu_usd",
        })
        fact_ee = df_eia_us.merge(
            df_wb_us[["period", "cpi_pct_yoy", "gdp_current_usd_mn",
                      "industrial_prod_usd", "unemployment_rate", "exchange_rate_lcu_usd"]],
            left_on="period_str", right_on="period", how="left"
        ).drop(columns=["period"], errors="ignore")
        fact_ee = fact_ee.merge(
            dim_time[["time_id", "period"]], left_on="period_str",
            right_on="period", how="left"
        ).drop(columns=["period"], errors="ignore")
        fact_ee = fact_ee.rename(columns={"period_str": "period"})
        fact_ee_cols = [
            "period", "time_id", "sector_id", "country_code", "year",
            "price_cents_kwh", "retail_sales_mwh", "revenue_million_usd",
            "cpi_pct_yoy", "gdp_current_usd_mn",
            "industrial_prod_usd", "unemployment_rate", "exchange_rate_lcu_usd",
        ]
        fact_ee = fact_ee[[c for c in fact_ee_cols if c in fact_ee.columns]]
        fact_ee = fact_ee.drop_duplicates(subset=["period", "sector_id", "country_code"]).reset_index(drop=True)

        # fact_generation
        fact_gen = df_gen[["period_str", "fuel_type_id", "fuel_name",
                            "fuel_category", "is_renewable", "net_generation_mwh", "year"]].copy()
        fact_gen = fact_gen.merge(
            dim_time[["time_id", "period"]], left_on="period_str",
            right_on="period", how="left"
        ).drop(columns=["period"], errors="ignore")
        fact_gen = fact_gen.rename(columns={"period_str": "period"})
        fact_gen = fact_gen.drop_duplicates(subset=["period", "fuel_type_id"]).reset_index(drop=True)

        # fact_worldbank_indicators
        wb_files_raw = {
            "cpi":           pd.read_csv(str(STAGING_DIR / "wb_cpi_2022_2024.csv")),
            "industrial":    pd.read_csv(str(STAGING_DIR / "wb_industrial_2022_2024.csv")),
            "exchange_rate": pd.read_csv(str(STAGING_DIR / "wb_exchange_rate_2022_2024.csv")),
            "unemployment":  pd.read_csv(str(STAGING_DIR / "wb_unemployment_2022_2024.csv")),
            "gdp":           pd.read_csv(str(STAGING_DIR / "wb_gdp_2022_2024.csv")),
        }
        df_wb_all = wb_files_raw["cpi"][["period", "country_code", "value"]].rename(columns={"value": "cpi_pct_yoy"})
        df_wb_all = df_wb_all.merge(
            wb_files_raw["industrial"][["period", "country_code", "value"]].rename(columns={"value": "industrial_prod_usd"}),
            on=["period", "country_code"], how="left"
        )
        df_wb_all = df_wb_all.merge(
            wb_files_raw["exchange_rate"][["period", "country_code", "value"]].rename(columns={"value": "exchange_rate_lcu_usd"}),
            on=["period", "country_code"], how="left"
        )
        df_wb_all = df_wb_all.merge(
            wb_files_raw["unemployment"][["period", "country_code", "value"]].rename(columns={"value": "unemployment_rate"}),
            on=["period", "country_code"], how="left"
        )
        df_gdp = wb_files_raw["gdp"][["period", "country_code", "value"]].rename(columns={"value": "gdp_current_usd_mn"})
        all_periods_str = [p.strftime("%Y-%m") for p in pd.date_range("2022-01-01", "2024-12-01", freq="MS")]
        country_codes   = df_wb_all["country_code"].unique().tolist()
        df_grid = pd.DataFrame([{"period": p, "country_code": c} for p in all_periods_str for c in country_codes])
        df_gdp_expanded = df_grid.merge(df_gdp, on=["period", "country_code"], how="left")
        df_gdp_expanded = df_gdp_expanded.sort_values(["country_code", "period"])
        df_gdp_expanded["gdp_current_usd_mn"] = df_gdp_expanded.groupby("country_code")["gdp_current_usd_mn"].ffill()
        df_wb_all = df_wb_all.merge(
            df_gdp_expanded[["period", "country_code", "gdp_current_usd_mn"]],
            on=["period", "country_code"], how="left"
        )
        df_wb_all["year"]    = df_wb_all["period"].str[:4].astype(int)
        df_wb_all["month"]   = df_wb_all["period"].str[5:7].astype(int)
        df_wb_all["quarter"] = ((df_wb_all["month"] - 1) // 3 + 1).astype(int)
        country_name_map = {
            "US": "United States", "CN": "China", "DE": "Germany", "JP": "Japan",
            "IN": "India", "GB": "United Kingdom", "FR": "France", "BR": "Brazil"
        }
        df_wb_all["country_name"] = df_wb_all["country_code"].map(country_name_map)
        df_wb_all = df_wb_all[[
            "period", "year", "quarter", "month", "country_code", "country_name",
            "cpi_pct_yoy", "industrial_prod_usd", "exchange_rate_lcu_usd",
            "unemployment_rate", "gdp_current_usd_mn"
        ]].drop_duplicates(subset=["period", "country_code"]).reset_index(drop=True)
        log.info("fact_worldbank_indicators shape=%s", df_wb_all.shape)

        # Simpan semua tabel ke clean/
        output_files = {
            "dim_time":                  dim_time,
            "dim_sector":                dim_sector,
            "dim_fuel_type":             dim_fuel_type,
            "dim_country":               dim_country,
            "fact_energy_economy":       fact_ee,
            "fact_generation":           fact_gen,
            "fact_worldbank_indicators": df_wb_all,
        }
        saved = []
        for name, df in output_files.items():
            path = str(CLEAN_DIR / f"{name}.csv")
            df.to_csv(path, index=False)
            saved.append(path)
            log.info("Transform %-22s ke %s  shape=%s", name, path, df.shape)

        log.info("Transform selesai. %d tabel tersimpan di %s", len(saved), CLEAN_DIR)
        return ",".join(saved)

    # TASK 3 - LOAD: Push ke Supabase
    @task(task_id="load_to_supabase")
    def load_to_supabase(clean_paths: str) -> str:
        db_url = os.environ.get("SUPABASE_DB_URL", "")
        if not db_url:
            raise EnvironmentError("SUPABASE_DB_URL tidak ditemukan.")

        engine = create_engine(db_url, pool_pre_ping=True)

        tables = {}
        for path in clean_paths.split(","):
            if not path:
                continue
            name = Path(path).stem
            tables[name] = pd.read_csv(path)
            log.info("Load: membaca %s  shape=%s", name, tables[name].shape)

        with engine.begin() as conn:

            # DDL
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_time (
                    time_id    INTEGER PRIMARY KEY,
                    period     VARCHAR(7)  NOT NULL,
                    year       INTEGER,
                    month      INTEGER,
                    quarter    INTEGER,
                    month_name VARCHAR(20)
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_sector (
                    sector_id   VARCHAR(5) PRIMARY KEY,
                    sector_name VARCHAR(50)
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_fuel_type (
                    fuel_id       SERIAL PRIMARY KEY,
                    fuel_type_id  VARCHAR(5)  UNIQUE NOT NULL,
                    fuel_name     VARCHAR(50),
                    fuel_category VARCHAR(20),
                    is_renewable  BOOLEAN
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_country (
                    country_id   SERIAL PRIMARY KEY,
                    country_code VARCHAR(3)  UNIQUE NOT NULL,
                    country_name VARCHAR(50),
                    region       VARCHAR(50),
                    income_group VARCHAR(20)
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_energy_economy (
                    period                  VARCHAR(7)  NOT NULL,
                    time_id                 INTEGER,
                    sector_id               VARCHAR(5)  NOT NULL,
                    country_code            VARCHAR(3)  NOT NULL,
                    year                    INTEGER,
                    price_cents_kwh         FLOAT,
                    retail_sales_mwh        FLOAT,
                    revenue_million_usd     FLOAT,
                    cpi_pct_yoy             FLOAT,
                    gdp_current_usd_mn      FLOAT,
                    industrial_prod_usd     FLOAT,
                    unemployment_rate       FLOAT,
                    exchange_rate_lcu_usd   FLOAT,
                    PRIMARY KEY (period, sector_id, country_code)
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_generation (
                    period              VARCHAR(7)  NOT NULL,
                    time_id             INTEGER,
                    fuel_type_id        VARCHAR(5)  NOT NULL,
                    fuel_name           VARCHAR(50),
                    fuel_category       VARCHAR(20),
                    is_renewable        BOOLEAN,
                    net_generation_mwh  FLOAT,
                    year                INTEGER,
                    PRIMARY KEY (period, fuel_type_id)
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_worldbank_indicators (
                    period                  VARCHAR(7)   NOT NULL,
                    year                    INTEGER,
                    quarter                 INTEGER,
                    month                   INTEGER,
                    country_code            VARCHAR(3)   NOT NULL,
                    country_name            VARCHAR(50),
                    cpi_pct_yoy             FLOAT,
                    industrial_prod_usd     FLOAT,
                    exchange_rate_lcu_usd   FLOAT,
                    unemployment_rate       FLOAT,
                    gdp_current_usd_mn      FLOAT,
                    PRIMARY KEY (period, country_code)
                );
            """))

            # UPSERT dim_time
            if "dim_time" in tables:
                for _, row in tables["dim_time"].iterrows():
                    conn.execute(text("""
                        INSERT INTO dim_time (time_id, period, year, month, quarter, month_name)
                        VALUES (:time_id, :period, :year, :month, :quarter, :month_name)
                        ON CONFLICT (time_id) DO UPDATE SET
                            period=EXCLUDED.period, year=EXCLUDED.year,
                            month=EXCLUDED.month, quarter=EXCLUDED.quarter,
                            month_name=EXCLUDED.month_name;
                    """), row.to_dict())
                log.info("dim_time loaded: %d rows", len(tables["dim_time"]))

            # UPSERT dim_sector
            if "dim_sector" in tables:
                for _, row in tables["dim_sector"].iterrows():
                    conn.execute(text("""
                        INSERT INTO dim_sector (sector_id, sector_name)
                        VALUES (:sector_id, :sector_name)
                        ON CONFLICT (sector_id) DO UPDATE SET sector_name=EXCLUDED.sector_name;
                    """), row.to_dict())
                log.info("dim_sector loaded: %d rows", len(tables["dim_sector"]))

            # UPSERT dim_fuel_type
            if "dim_fuel_type" in tables:
                for _, row in tables["dim_fuel_type"].iterrows():
                    conn.execute(text("""
                        INSERT INTO dim_fuel_type (fuel_type_id, fuel_name, fuel_category, is_renewable)
                        VALUES (:fuel_type_id, :fuel_name, :fuel_category, :is_renewable)
                        ON CONFLICT (fuel_type_id) DO UPDATE SET
                            fuel_name=EXCLUDED.fuel_name,
                            fuel_category=EXCLUDED.fuel_category,
                            is_renewable=EXCLUDED.is_renewable;
                    """), row.to_dict())
                log.info("dim_fuel_type loaded: %d rows", len(tables["dim_fuel_type"]))

            # UPSERT dim_country
            if "dim_country" in tables:
                for _, row in tables["dim_country"].iterrows():
                    conn.execute(text("""
                        INSERT INTO dim_country (country_code, country_name, region, income_group)
                        VALUES (:country_code, :country_name, :region, :income_group)
                        ON CONFLICT (country_code) DO UPDATE SET
                            country_name=EXCLUDED.country_name,
                            region=EXCLUDED.region,
                            income_group=EXCLUDED.income_group;
                    """), row.to_dict())
                log.info("dim_country loaded: %d rows", len(tables["dim_country"]))

            # IDEMPOTENT fact_energy_economy
            if "fact_energy_economy" in tables:
                df_fee = tables["fact_energy_economy"]
                periods = df_fee["period"].dropna().unique().tolist()
                if periods:
                    conn.execute(text("DELETE FROM fact_energy_economy WHERE period = ANY(:periods)"),
                                 {"periods": periods})
                    df_fee.where(pd.notnull(df_fee), None).to_sql(
                        "fact_energy_economy", conn, if_exists="append",
                        index=False, method="multi", chunksize=500)
                log.info("fact_energy_economy loaded: %d rows", len(df_fee))

            # IDEMPOTENT fact_generation
            if "fact_generation" in tables:
                df_fgen = tables["fact_generation"]
                periods = df_fgen["period"].dropna().unique().tolist()
                if periods:
                    conn.execute(text("DELETE FROM fact_generation WHERE period = ANY(:periods)"),
                                 {"periods": periods})
                    df_fgen.where(pd.notnull(df_fgen), None).to_sql(
                        "fact_generation", conn, if_exists="append",
                        index=False, method="multi", chunksize=500)
                log.info("fact_generation loaded: %d rows", len(df_fgen))

            # IDEMPOTENT fact_worldbank_indicators
            if "fact_worldbank_indicators" in tables:
                df_fwb = tables["fact_worldbank_indicators"]
                periods = df_fwb["period"].dropna().unique().tolist()
                if periods:
                    conn.execute(text("DELETE FROM fact_worldbank_indicators WHERE period = ANY(:periods)"),
                                 {"periods": periods})
                    df_fwb.where(pd.notnull(df_fwb), None).to_sql(
                        "fact_worldbank_indicators", conn, if_exists="append",
                        index=False, method="multi", chunksize=500)
                log.info("fact_worldbank_indicators loaded: %d rows", len(df_fwb))

        log.info("=== LOAD KE SUPABASE SELESAI ===")
        return "load_success"

    # TASK 4 - REFRESH Materialized Views
    @task(task_id="refresh_materialized_views")
    def refresh_materialized_views(load_status: str) -> str:
        if load_status != "load_success":
            log.warning("Load tidak sukses, skip refresh views.")
            return "skipped"

        db_url = os.environ.get("SUPABASE_DB_URL", "")
        if not db_url:
            raise EnvironmentError("SUPABASE_DB_URL tidak ditemukan.")

        engine = create_engine(db_url, pool_pre_ping=True)

        with engine.begin() as conn:
            try:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"))
                log.info("pg_stat_statements extension OK")
            except Exception as e:
                log.warning("pg_stat_statements: %s", e)

            conn.execute(text("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS mv_generation_monthly AS
                SELECT fg.period, fg.year, dt.quarter, fg.fuel_type_id, fg.fuel_name,
                       fg.fuel_category, fg.is_renewable,
                       SUM(fg.net_generation_mwh) AS total_generation_mwh,
                       COUNT(*) AS record_count
                FROM fact_generation fg
                LEFT JOIN dim_time dt ON fg.period = dt.period
                GROUP BY fg.period, fg.year, dt.quarter, fg.fuel_type_id,
                         fg.fuel_name, fg.fuel_category, fg.is_renewable
                ORDER BY fg.period, fg.fuel_type_id;
            """))
            log.info("mv_generation_monthly: DDL OK")

            conn.execute(text("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS mv_retail_economic AS
                SELECT fee.period, fee.year, dt.quarter, fee.sector_id, ds.sector_name,
                       AVG(fee.price_cents_kwh)     AS avg_price_cents_kwh,
                       SUM(fee.retail_sales_mwh)    AS total_sales_mwh,
                       SUM(fee.revenue_million_usd) AS total_revenue_usd_mn,
                       AVG(fee.cpi_pct_yoy)         AS avg_cpi_pct_yoy,
                       AVG(fee.gdp_current_usd_mn)  AS avg_gdp_usd_mn,
                       AVG(fee.unemployment_rate)   AS avg_unemployment_rate
                FROM fact_energy_economy fee
                LEFT JOIN dim_time   dt ON fee.period    = dt.period
                LEFT JOIN dim_sector ds ON fee.sector_id = ds.sector_id
                GROUP BY fee.period, fee.year, dt.quarter, fee.sector_id, ds.sector_name
                ORDER BY fee.period, fee.sector_id;
            """))
            log.info("mv_retail_economic: DDL OK")

            try:
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_fee_period  ON fact_energy_economy (period);
                    CREATE INDEX IF NOT EXISTS idx_fee_sector  ON fact_energy_economy (sector_id);
                    CREATE INDEX IF NOT EXISTS idx_fgen_period ON fact_generation (period);
                    CREATE INDEX IF NOT EXISTS idx_fgen_fuel   ON fact_generation (fuel_type_id);
                """))
                log.info("Index berhasil dibuat/diverifikasi")
            except Exception as e:
                log.warning("Index creation: %s", e)

        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn2:
            conn2.execute(text("REFRESH MATERIALIZED VIEW mv_generation_monthly;"))
            log.info("mv_generation_monthly: REFRESHED")

        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn3:
            conn3.execute(text("REFRESH MATERIALIZED VIEW mv_retail_economic;"))
            log.info("mv_retail_economic: REFRESHED")

        log.info("=== REFRESH MATERIALIZED VIEWS SELESAI ===")
        return "refresh_success"

    # DEPENDENCY GRAPH
    t_retail  = extract_eia_retail()
    t_gen     = extract_eia_generation()
    t_wb      = extract_worldbank()

    t_transform = transform_all(
        retail_path=t_retail,
        gen_path=t_gen,
        wb_paths=t_wb
    )

    t_load = load_to_supabase(clean_paths=t_transform)

    refresh_materialized_views(load_status=t_load)


energy_economy_etl()
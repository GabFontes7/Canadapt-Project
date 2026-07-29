"""
CanAdapt — Bronze cost-of-living ingestion (official sources + Dual-Write).

Direct, reproducible sources:
  1) CMHC Rental Market Survey 2025 HTML tables
  2) Statistics Canada SHS 2023 + CPI 2026 downloadable CSV tables
  3) CRA/Revenu Québec consumption-tax rates effective in 2026

Dual-Write: local data/bronze/... and AWS S3 bronze/cost_of_living/...
"""

from __future__ import annotations

import json
import logging
import os
import sys
import unicodedata
import zipfile
from datetime import datetime, timezone
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, Literal

import boto3
import pandas as pd
import requests
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError, field_validator

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"

REFERENCE_YEAR = 2026

CMHC_PROVINCE_IDS = {
    "NL": "10",
    "PE": "11",
    "NS": "12",
    "NB": "13",
    "QC": "24",
    "ON": "35",
    "MB": "46",
    "SK": "47",
    "AB": "48",
    "BC": "59",
}

CMHC_CITY_LABELS = {
    "Toronto": "Toronto",
    "Ottawa": "Ottawa",
    "Waterloo": "Kitchener - Cambridge - Waterloo",
    "Vancouver": "Vancouver",
    "Victoria": "Victoria",
    "Calgary": "Calgary",
    "Edmonton": "Edmonton",
    "Montréal": "Montréal",
    "Québec City": "Québec",
    "Halifax": "Halifax",
    "Winnipeg": "Winnipeg",
    "Saskatoon": "Saskatoon",
    "Regina": "Regina",
    "Moncton": "Moncton",
    "Fredericton": "Fredericton",
    "Charlottetown": "Charlottetown",
    "St. John's": "St. John's",
}

PROVINCE_CONFIG = {
    "ON": ("Ontario", 0.05, 0.08, 0.13, ("Toronto", "Ottawa", "Waterloo")),
    "BC": ("British Columbia", 0.05, 0.07, 0.12, ("Vancouver", "Victoria")),
    "AB": ("Alberta", 0.05, 0.00, 0.05, ("Calgary", "Edmonton")),
    "QC": ("Québec", 0.05, 0.09975, 0.14975, ("Montréal", "Québec City")),
    "NS": ("Nova Scotia", 0.05, 0.09, 0.14, ("Halifax",)),
    "MB": ("Manitoba", 0.05, 0.07, 0.12, ("Winnipeg",)),
    "SK": ("Saskatchewan", 0.05, 0.06, 0.11, ("Saskatoon", "Regina")),
    "NB": ("New Brunswick", 0.05, 0.10, 0.15, ("Moncton", "Fredericton")),
    "PE": ("Prince Edward Island", 0.05, 0.10, 0.15, ("Charlottetown",)),
    "NL": ("Newfoundland and Labrador", 0.05, 0.10, 0.15, ("St. John's",)),
}

CRA_SALES_TAX_URL = (
    "https://www.canada.ca/en/revenue-agency/services/tax/businesses/topics/"
    "gst-hst-businesses/charge-collect-which-rate.html"
)
STATCAN_SHS_URL = "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1110022201"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("canadapt.ingestion.cost_of_living")


# -----------------------------------------------------------------------------
# Pydantic schemas (Structured Output)
# -----------------------------------------------------------------------------


class CityCost(BaseModel):
    nome_cidade: str
    aluguel_medio_1bdr: float = Field(
        ge=300,
        le=10000,
        description="Aluguel de 1 quarto do CMHC, com ano da fonte explícito"
    )
    custo_vida_sem_aluguel: float = Field(
        ge=300,
        le=10000,
        description="Estimativa mensal auditável baseada no StatCan SHS + CPI"
    )
    fonte_moradia_verificada: str = Field(
        description="Nome da fonte/relatório de moradia encontrado"
    )
    fonte_custo_vida_verificada: str = Field(
        description="Nome da fonte de custo de vida encontrada"
    )
    fonte_moradia_url: str
    fonte_custo_vida_url: str
    ano_fonte_moradia: int
    ano_fonte_custo_vida: int
    qualidade_fonte_moradia: Literal["a", "b", "c", "d", "unknown"]
    metodo_custo_vida: Literal["statcan_shs_cpi_provincial"]
    custo_vida_estimado: bool = True
    custo_base_provincial_2023: float | None = None
    fator_domicilio_unipessoal: float | None = None
    fator_cpi_2026: float | None = None
    cpi_mes_referencia: str | None = None

    @field_validator("fonte_moradia_url")
    @classmethod
    def validate_cmhc_url(cls, value: str) -> str:
        if "cmhc-schl.gc.ca" not in value:
            raise ValueError("Housing source must be an official CMHC URL")
        return value

    @field_validator("fonte_custo_vida_url")
    @classmethod
    def validate_statcan_url(cls, value: str) -> str:
        if "statcan.gc.ca" not in value:
            raise ValueError("Cost source must be an official StatCan URL")
        return value


class ProvinceTax(BaseModel):
    sigla_provincia: str = Field(
        description="ON, BC, AB, QC, NS, MB, SK, NB, PE, NL"
    )
    nome_provincia: str
    aliquota_gst: float = Field(description="Imposto federal (geralmente 0.05)")
    aliquota_pst: float = Field(description="Imposto provincial (ex: 0.08 para ON)")
    aliquota_hst_total: float = Field(
        description="Soma de GST + PST, ou taxa unificada se HST"
    )
    fonte_imposto_url: str
    vigencia_imposto: str
    cidades: list[CityCost]

    @field_validator("fonte_imposto_url")
    @classmethod
    def validate_tax_url(cls, value: str) -> str:
        if "canada.ca" not in value and "revenuquebec.ca" not in value:
            raise ValueError("Tax source must be CRA or Revenu Québec")
        return value


class CostOfLivingPayload(BaseModel):
    ano_referencia: int = Field(description="Deve ser obrigatoriamente 2026")
    data_execucao: str = Field(description="Data atual no formato YYYY-MM-DD")
    consultado_em_utc: str
    metodologia_versao: Literal["official_sources_v2"]
    provincias: list[ProvinceTax]


# -----------------------------------------------------------------------------
# Direct official CMHC extraction
# -----------------------------------------------------------------------------


def _normalize_label(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    ascii_text = "".join(char for char in decomposed if not unicodedata.combining(char))
    return " ".join(ascii_text.casefold().split())


def _cmhc_url(province_code: str) -> str:
    return (
        "https://www03.cmhc-schl.gc.ca/hmip-pimh/en/TableMapChart/"
        "TableCategory?categoryLevel1=Primary+Rental+Market"
        "&categoryLevel2=Average+Rent+%28%24%29"
        f"&geographyId={CMHC_PROVINCE_IDS[province_code]}"
        "&geographyType=Province"
    )


def fetch_cmhc_rents_2025() -> dict[str, dict[str, Any]]:
    """Read one-bedroom rents directly from official CMHC HTML tables."""
    result: dict[str, dict[str, Any]] = {}
    city_to_province = {
        city: province
        for province, cities in {
            "ON": ("Toronto", "Ottawa", "Waterloo"),
            "BC": ("Vancouver", "Victoria"),
            "AB": ("Calgary", "Edmonton"),
            "QC": ("Montréal", "Québec City"),
            "NS": ("Halifax",),
            "MB": ("Winnipeg",),
            "SK": ("Saskatoon", "Regina"),
            "NB": ("Moncton", "Fredericton"),
            "PE": ("Charlottetown",),
            "NL": ("St. John's",),
        }.items()
        for city in cities
    }

    for province_code in sorted(set(city_to_province.values())):
        url = _cmhc_url(province_code)
        response = requests.get(url, timeout=45)
        response.raise_for_status()
        tables = pd.read_html(StringIO(response.text))
        if not tables:
            raise RuntimeError(f"CMHC returned no table for {province_code}")
        table = tables[0]
        city_column = table.columns[0]
        normalized_rows = {
            _normalize_label(str(row[city_column])): row
            for _, row in table.iterrows()
        }
        for city, expected_province in city_to_province.items():
            if expected_province != province_code:
                continue
            label = CMHC_CITY_LABELS[city]
            row = normalized_rows.get(_normalize_label(label))
            if row is None:
                raise RuntimeError(f"CMHC city not found: {city} ({province_code})")
            raw_rent = str(row["1 Bedroom"]).replace(",", "").strip()
            if raw_rent in {"**", "nan", ""}:
                raise RuntimeError(f"CMHC one-bedroom rent suppressed for {city}")
            rent = float(raw_rent)
            quality_raw = str(row.get("1 Bedroom.1", "unknown")).strip().lower()
            quality = quality_raw if quality_raw in {"a", "b", "c", "d"} else "unknown"
            result[city] = {
                "rent": rent,
                "quality": quality,
                "url": url,
            }

    if set(result) != set(CMHC_CITY_LABELS):
        missing = sorted(set(CMHC_CITY_LABELS) - set(result))
        raise RuntimeError(f"CMHC extraction incomplete; missing={missing}")
    logger.info("CMHC direct extraction complete | cities=%d | edition=2025", len(result))
    return result


def apply_official_cmhc_rents(
    payload: CostOfLivingPayload,
    rents: dict[str, dict[str, Any]],
) -> CostOfLivingPayload:
    """Replace any LLM rent value with the directly parsed CMHC observation."""
    for province in payload.provincias:
        for city in province.cidades:
            official = rents[city.nome_cidade]
            city.aluguel_medio_1bdr = official["rent"]
            city.qualidade_fonte_moradia = official["quality"]
            city.fonte_moradia_url = official["url"]
            city.fonte_moradia_verificada = (
                "CMHC Rental Market Survey 2025 — Primary Rental Market"
            )
            city.ano_fonte_moradia = 2025
    return payload


def _download_statcan_table(table_id: str) -> pd.DataFrame:
    url = f"https://www150.statcan.gc.ca/n1/en/tbl/csv/{table_id}-eng.zip"
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    with zipfile.ZipFile(BytesIO(response.content)) as archive:
        csv_name = next(
            name
            for name in archive.namelist()
            if name.endswith(".csv") and "MetaData" not in name
        )
        return pd.read_csv(archive.open(csv_name), low_memory=False)


def fetch_statcan_single_person_costs_2026() -> dict[str, dict[str, Any]]:
    """Derive province-level monthly non-shelter cost from official StatCan tables."""
    category = "Household expenditures, summary-level categories"
    shs_province = _download_statcan_table("11100222")
    shs_type = _download_statcan_table("11100224")
    cpi = _download_statcan_table("18100004")

    def value(
        frame: pd.DataFrame,
        *,
        geo: str,
        expense: str,
        household_type: str | None = None,
    ) -> float:
        mask = (
            (frame["REF_DATE"] == 2023)
            & (frame["GEO"] == geo)
            & (frame["Statistic"] == "Average expenditure per household")
            & (frame[category] == expense)
        )
        if household_type is not None:
            mask &= frame["Household type"] == household_type
        values = frame.loc[mask, "VALUE"].dropna()
        if len(values) != 1:
            raise RuntimeError(
                f"Unexpected StatCan cardinality: geo={geo}, expense={expense}, "
                f"household_type={household_type}, rows={len(values)}"
            )
        return float(values.iloc[0])

    all_non_shelter = value(
        shs_type,
        geo="Canada",
        expense="Total current consumption",
        household_type="All classes",
    ) - value(
        shs_type,
        geo="Canada",
        expense="Shelter",
        household_type="All classes",
    )
    single_non_shelter = value(
        shs_type,
        geo="Canada",
        expense="Total current consumption",
        household_type="One person households",
    ) - value(
        shs_type,
        geo="Canada",
        expense="Shelter",
        household_type="One person households",
    )
    single_factor = single_non_shelter / all_non_shelter

    cpi_all = cpi[
        (cpi["GEO"] == "Canada")
        & (cpi["Products and product groups"] == "All-items")
    ].copy()
    cpi_all["year"] = cpi_all["REF_DATE"].astype(str).str[:4].astype(int)
    cpi_2023 = float(cpi_all.loc[cpi_all["year"] == 2023, "VALUE"].mean())
    cpi_2026_rows = cpi_all.loc[cpi_all["year"] == 2026].sort_values("REF_DATE")
    if cpi_2026_rows.empty:
        raise RuntimeError("StatCan CPI has no 2026 observations")
    cpi_2026 = float(cpi_2026_rows["VALUE"].mean())
    cpi_factor = cpi_2026 / cpi_2023
    cpi_reference = str(cpi_2026_rows["REF_DATE"].iloc[-1])

    province_names = {
        "ON": "Ontario",
        "BC": "British Columbia",
        "AB": "Alberta",
        "QC": "Quebec",
        "NS": "Nova Scotia",
        "MB": "Manitoba",
        "SK": "Saskatchewan",
        "NB": "New Brunswick",
        "PE": "Prince Edward Island",
        "NL": "Newfoundland and Labrador",
    }
    costs: dict[str, dict[str, Any]] = {}
    for code, geo in province_names.items():
        current = value(
            shs_province,
            geo=geo,
            expense="Total current consumption",
        )
        shelter = value(shs_province, geo=geo, expense="Shelter")
        province_non_shelter = current - shelter
        costs[code] = {
            "monthly": round(province_non_shelter * single_factor * cpi_factor / 12, 2),
            "base_2023": province_non_shelter,
            "single_factor": single_factor,
            "cpi_factor": cpi_factor,
            "cpi_reference": cpi_reference,
        }
    logger.info(
        "StatCan direct derivation complete | provinces=%d | single_factor=%.4f | "
        "cpi_factor=%.4f | latest_cpi=%s",
        len(costs),
        single_factor,
        cpi_factor,
        cpi_reference,
    )
    return costs


def apply_official_statcan_costs(
    payload: CostOfLivingPayload,
    costs: dict[str, dict[str, Any]],
) -> CostOfLivingPayload:
    source_url = "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1110022201"
    for province in payload.provincias:
        official = costs[province.sigla_provincia]
        for city in province.cidades:
            city.custo_vida_sem_aluguel = official["monthly"]
            city.fonte_custo_vida_url = source_url
            city.fonte_custo_vida_verificada = (
                "Statistics Canada SHS tables 11-10-0222-01 and 11-10-0224-01; "
                "CPI table 18-10-0004-01"
            )
            city.ano_fonte_custo_vida = 2023
            city.metodo_custo_vida = "statcan_shs_cpi_provincial"
            city.custo_vida_estimado = True
            city.custo_base_provincial_2023 = official["base_2023"]
            city.fator_domicilio_unipessoal = official["single_factor"]
            city.fator_cpi_2026 = official["cpi_factor"]
            city.cpi_mes_referencia = official["cpi_reference"]
    return payload


def build_official_payload(
    now: datetime,
    rents: dict[str, dict[str, Any]],
    costs: dict[str, dict[str, Any]],
) -> CostOfLivingPayload:
    """Build the contract without LLM-generated monetary or tax values."""
    provinces: list[ProvinceTax] = []
    for code, (
        name,
        gst,
        pst,
        total_tax,
        city_names,
    ) in PROVINCE_CONFIG.items():
        province_cost = costs[code]
        cities = []
        for city_name in city_names:
            rent = rents[city_name]
            cities.append(
                CityCost(
                    nome_cidade=city_name,
                    aluguel_medio_1bdr=rent["rent"],
                    custo_vida_sem_aluguel=province_cost["monthly"],
                    fonte_moradia_verificada=(
                        "CMHC Rental Market Survey 2025 — Primary Rental Market"
                    ),
                    fonte_custo_vida_verificada=(
                        "Statistics Canada SHS tables 11-10-0222-01 and "
                        "11-10-0224-01; CPI table 18-10-0004-01"
                    ),
                    fonte_moradia_url=rent["url"],
                    fonte_custo_vida_url=STATCAN_SHS_URL,
                    ano_fonte_moradia=2025,
                    ano_fonte_custo_vida=2023,
                    qualidade_fonte_moradia=rent["quality"],
                    metodo_custo_vida="statcan_shs_cpi_provincial",
                    custo_vida_estimado=True,
                    custo_base_provincial_2023=province_cost["base_2023"],
                    fator_domicilio_unipessoal=province_cost["single_factor"],
                    fator_cpi_2026=province_cost["cpi_factor"],
                    cpi_mes_referencia=province_cost["cpi_reference"],
                )
            )
        provinces.append(
            ProvinceTax(
                sigla_provincia=code,
                nome_provincia=name,
                aliquota_gst=gst,
                aliquota_pst=pst,
                aliquota_hst_total=total_tax,
                fonte_imposto_url=CRA_SALES_TAX_URL,
                vigencia_imposto="2026",
                cidades=cities,
            )
        )
    return CostOfLivingPayload(
        ano_referencia=REFERENCE_YEAR,
        data_execucao=now.strftime("%Y-%m-%d"),
        consultado_em_utc=datetime.now(timezone.utc).isoformat(),
        metodologia_versao="official_sources_v2",
        provincias=provinces,
    )


# -----------------------------------------------------------------------------
# Env / AWS helpers
# -----------------------------------------------------------------------------


def load_env() -> None:
    load_dotenv(PROJECT_ROOT / ".env")


def resolve_s3_bucket_name() -> str:
    """Prefer AWS_BUCKET_NAME; fall back to AWS_S3_BUCKET_NAME (Adzuna harmony)."""
    bucket = (
        os.getenv("AWS_BUCKET_NAME", "").strip()
        or os.getenv("AWS_S3_BUCKET_NAME", "").strip()
    )
    if not bucket:
        raise EnvironmentError(
            "Missing AWS_BUCKET_NAME (or fallback AWS_S3_BUCKET_NAME) in .env."
        )
    return bucket


def build_s3_client():
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
    region = os.getenv("AWS_DEFAULT_REGION", "").strip() or "us-east-1"

    if not access_key or not secret_key:
        raise EnvironmentError(
            "Missing AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY in .env."
        )

    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )


def partition_paths(now: datetime) -> tuple[Path, str]:
    """Dynamic Hive partitions for local disk and S3."""
    year_p = f"year={now.year}"
    month_p = f"month={now.month:02d}"
    day_p = f"day={now.day:02d}"
    filename = "cost_of_living.json"

    local_path = BRONZE_ROOT / year_p / month_p / day_p / filename
    s3_key = f"bronze/cost_of_living/{year_p}/{month_p}/{day_p}/{filename}"
    return local_path, s3_key


# -----------------------------------------------------------------------------
# Dual-Write
# -----------------------------------------------------------------------------


def save_local(payload: CostOfLivingPayload, local_path: Path) -> Path:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(
        payload.model_dump_json(indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("Bronze local written: %s", local_path)
    return local_path


def save_s3(payload: CostOfLivingPayload, s3_key: str) -> str:
    bucket = resolve_s3_bucket_name()
    body = payload.model_dump_json(indent=2, ensure_ascii=False)

    try:
        s3 = build_s3_client()
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=body.encode("utf-8"),
            ContentType="application/json; charset=utf-8",
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        raise RuntimeError(
            f"S3 put_object failed ({code}) for s3://{bucket}/{s3_key}: {exc}"
        ) from exc
    except BotoCoreError as exc:
        raise RuntimeError(f"AWS/boto3 error uploading to S3: {exc}") from exc

    uri = f"s3://{bucket}/{s3_key}"
    logger.info("Bronze S3 written: %s", uri)
    return uri


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def main() -> int:
    load_env()
    now = datetime.now().astimezone()
    local_path, s3_key = partition_paths(now)

    try:
        payload = build_official_payload(
            now,
            fetch_cmhc_rents_2025(),
            fetch_statcan_single_person_costs_2026(),
        )

        save_local(payload, local_path)
        s3_uri = save_s3(payload, s3_key)
    except (EnvironmentError, RuntimeError, ValidationError, OSError) as exc:
        logger.error("Cost-of-living ingestion failed: %s", exc)
        return 1

    summary: dict[str, Any] = {
        "ano_referencia": payload.ano_referencia,
        "data_execucao": payload.data_execucao,
        "metodologia_versao": payload.metodologia_versao,
        "provincias": len(payload.provincias),
        "cidades": sum(len(p.cidades) for p in payload.provincias),
        "local": str(local_path),
        "s3": s3_uri,
    }
    logger.info("Dual-Write finished | %s", summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
CanAdapt — Bronze cost-of-living ingestion (Gemini agent + Dual-Write).

Two-step production pattern:
  1) gemini-3.1-flash-lite + Google Search → textual research report (2026 CA data)
  2) gemini-3.1-flash-lite + Pydantic response_schema → CostOfLivingPayload JSON

Dual-Write: local data/bronze/... and AWS S3 bronze/cost_of_living/...
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from google import genai
from google.genai import types
from google.genai.errors import ClientError as GeminiClientError
from pydantic import BaseModel, Field, ValidationError

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"

DEFAULT_GEMINI_MODEL = "gemini-3.1-flash-lite"
REFERENCE_YEAR = 2026
GEMINI_MAX_RETRIES = 5
GEMINI_RETRY_BASE_DELAY_SECONDS = 20
# Retryable Gemini API statuses (quota/rate + transient).
GEMINI_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

GEOGRAPHIC_SCOPE = """
ON (Ontario): Toronto, Ottawa, Waterloo
BC (British Columbia): Vancouver, Victoria
AB (Alberta): Calgary, Edmonton
QC (Québec): Montréal, Québec City
NS (Nova Scotia): Halifax
MB (Manitoba): Winnipeg
SK (Saskatchewan): Saskatoon, Regina
NB (New Brunswick): Moncton, Fredericton
PE (Prince Edward Island): Charlottetown
NL (Newfoundland and Labrador): St. John's
""".strip()

RESEARCH_PROMPT = f"""
Pesquise na internet as tabelas oficiais da Canada Revenue Agency (CRA) para taxas
de impostos de consumo (GST, PST, HST) vigentes em {REFERENCE_YEAR} para todas as
10 províncias do Canadá.

Além disso, busque os dados reais mais recentes de aluguel médio de apartamentos
de 1 quarto (1-Bedroom) do CMHC (Canada Mortgage and Housing Corporation) de
{REFERENCE_YEAR}, e a média de custo de vida mensal (sem aluguel) para uma pessoa
solteira em {REFERENCE_YEAR} nas cidades especificadas abaixo.

Escopo geográfico obrigatório (todas as cidades devem ser cobertas):
{GEOGRAPHIC_SCOPE}

Entregue um relatório textual consolidado, claro e citável, contendo:
- GST / PST / HST por província (valores numéricos e se a província usa HST unificado)
- Aluguel médio 1-bedroom por cidade (CMHC ou fonte oficial equivalente mais recente)
- Custo de vida mensal sem aluguel para solteiro por cidade (StatCan / Numbeo / equivalentes)
- Nome exato das fontes/relatórios encontrados para moradia e custo de vida

Se algum valor de {REFERENCE_YEAR} ainda não estiver publicado, use o dado oficial mais
recente disponível e declare explicitamente o ano da fonte.
""".strip()

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
        description="Média de aluguel de 1 quarto baseada no CMHC de 2026"
    )
    custo_vida_sem_aluguel: float = Field(
        description="Estimativa de custo de vida mensal de solteiro (StatCan/Numbeo 2026)"
    )
    fonte_moradia_verificada: str = Field(
        description="Nome da fonte/relatório de moradia encontrado"
    )
    fonte_custo_vida_verificada: str = Field(
        description="Nome da fonte de custo de vida encontrada"
    )


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
    cidades: list[CityCost]


class CostOfLivingPayload(BaseModel):
    ano_referencia: int = Field(description="Deve ser obrigatoriamente 2026")
    data_execucao: str = Field(description="Data atual no formato YYYY-MM-DD")
    provincias: list[ProvinceTax]


# -----------------------------------------------------------------------------
# Env / AWS helpers
# -----------------------------------------------------------------------------


def load_env() -> None:
    load_dotenv(PROJECT_ROOT / ".env")


def resolve_gemini_model() -> str:
    """Allow override via GEMINI_MODEL; default to current Flash GA for new keys."""
    return os.getenv("GEMINI_MODEL", DEFAULT_GEMINI_MODEL).strip() or DEFAULT_GEMINI_MODEL


def require_gemini_api_key() -> str:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise EnvironmentError(
            "Missing GEMINI_API_KEY in the project .env file."
        )
    return api_key


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
# Gemini two-step agent
# -----------------------------------------------------------------------------


def _gemini_error_code(exc: BaseException) -> int | None:
    code = getattr(exc, "code", None)
    return int(code) if isinstance(code, int) else None


def generate_content_with_retry(
    label: str,
    call: Callable[[], Any],
) -> Any:
    """Call Gemini with exponential backoff on 429 / transient 5xx."""
    last_error: Exception | None = None

    for attempt in range(1, GEMINI_MAX_RETRIES + 1):
        try:
            return call()
        except GeminiClientError as exc:
            status = _gemini_error_code(exc)
            if status not in GEMINI_RETRYABLE_STATUS_CODES or attempt >= GEMINI_MAX_RETRIES:
                raise
            delay = GEMINI_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "%s | Gemini HTTP %s (attempt %d/%d). Backoff %ss…",
                label,
                status,
                attempt,
                GEMINI_MAX_RETRIES,
                delay,
            )
            time.sleep(delay)
            last_error = exc
        except Exception as exc:  # noqa: BLE001
            # Some SDK wraps may not expose ClientError; detect 429 in message.
            msg = str(exc)
            is_quota = "429" in msg or "RESOURCE_EXHAUSTED" in msg
            if not is_quota or attempt >= GEMINI_MAX_RETRIES:
                raise
            delay = GEMINI_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "%s | Gemini quota/rate limit (attempt %d/%d). Backoff %ss…",
                label,
                attempt,
                GEMINI_MAX_RETRIES,
                delay,
            )
            time.sleep(delay)
            last_error = exc

    assert last_error is not None
    raise last_error


def etapa_1_pesquisa(client: genai.Client, model: str) -> str:
    """Step 1: Google Search grounding → consolidated textual research report."""
    logger.info("Etapa 1/2 | Gemini + Google Search | model=%s", model)
    try:
        response = generate_content_with_retry(
            "Etapa 1/2",
            lambda: client.models.generate_content(
                model=model,
                contents=RESEARCH_PROMPT,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    temperature=0.2,
                ),
            ),
        )
    except Exception as exc:  # noqa: BLE001 — surface Gemini API failures clearly
        raise RuntimeError(f"Gemini research call failed: {exc}") from exc

    text = (response.text or "").strip()
    if not text:
        raise RuntimeError("Gemini research call returned an empty report.")

    logger.info("Etapa 1/2 | relatório textual recebido | chars=%d", len(text))
    return text


def etapa_2_estruturar(
    client: genai.Client,
    model: str,
    research_report: str,
    execution_date: str,
) -> CostOfLivingPayload:
    """Step 2: structure the research report into CostOfLivingPayload via schema."""
    logger.info(
        "Etapa 2/2 | Gemini structured output | model=%s | schema=CostOfLivingPayload",
        model,
    )

    structure_prompt = f"""
Com base EXCLUSIVAMENTE no relatório de pesquisa abaixo, preencha a estrutura JSON
solicitada (CostOfLivingPayload).

Regras obrigatórias:
- ano_referencia deve ser {REFERENCE_YEAR}
- data_execucao deve ser "{execution_date}"
- Inclua as 10 províncias canadenses e TODAS as cidades do escopo definido
- Valores numéricos devem ser floats puros (sem símbolos de moeda ou %):
  * alíquotas como fração (ex.: 0.05 para 5%, 0.13 para 13% HST)
  * valores monetários mensais em CAD (ex.: 1850.0)
- Cite nas strings de fonte o nome real do relatório/fonte encontrado no relatório
- Não invente cidades fora do escopo; não omita cidades do escopo

Escopo geográfico:
{GEOGRAPHIC_SCOPE}

RELATÓRIO DE PESQUISA:
---
{research_report}
---
""".strip()

    try:
        response = generate_content_with_retry(
            "Etapa 2/2",
            lambda: client.models.generate_content(
                model=model,
                contents=structure_prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=CostOfLivingPayload,
                    temperature=0.1,
                ),
            ),
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Gemini structured-output call failed: {exc}") from exc

    raw = (response.text or "").strip()
    if not raw:
        raise RuntimeError("Gemini structured-output call returned empty content.")

    try:
        payload = CostOfLivingPayload.model_validate_json(raw)
    except ValidationError as exc:
        raise RuntimeError(
            f"Pydantic validation failed for CostOfLivingPayload: {exc}"
        ) from exc

    if payload.ano_referencia != REFERENCE_YEAR:
        raise RuntimeError(
            f"ano_referencia must be {REFERENCE_YEAR}, got {payload.ano_referencia}."
        )

    logger.info(
        "Etapa 2/2 | JSON validado | provincias=%d | cidades=%d",
        len(payload.provincias),
        sum(len(p.cidades) for p in payload.provincias),
    )
    return payload


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
    execution_date = now.strftime("%Y-%m-%d")
    local_path, s3_key = partition_paths(now)

    try:
        api_key = require_gemini_api_key()
        model = resolve_gemini_model()
        client = genai.Client(api_key=api_key)

        research_report = etapa_1_pesquisa(client, model)
        payload = etapa_2_estruturar(client, model, research_report, execution_date)

        save_local(payload, local_path)
        s3_uri = save_s3(payload, s3_key)
    except (EnvironmentError, RuntimeError, ValidationError, OSError) as exc:
        logger.error("Cost-of-living ingestion failed: %s", exc)
        return 1

    summary: dict[str, Any] = {
        "ano_referencia": payload.ano_referencia,
        "data_execucao": payload.data_execucao,
        "provincias": len(payload.provincias),
        "cidades": sum(len(p.cidades) for p in payload.provincias),
        "local": str(local_path),
        "s3": s3_uri,
    }
    logger.info("Dual-Write finished | %s", summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())

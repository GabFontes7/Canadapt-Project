"""CanAdapt — entrada Streamlit (apresentação + portal de vagas)."""

from __future__ import annotations

import os
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
AWS_SECRET_KEYS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
    "AWS_BUCKET_NAME",
    "AWS_S3_BUCKET_NAME",
)


def _apply_runtime_secrets() -> None:
    load_dotenv(ROOT / ".env")
    for key in AWS_SECRET_KEYS:
        if os.getenv(key, "").strip():
            continue
        try:
            value = st.secrets[key]
        except Exception:  # noqa: BLE001
            continue
        os.environ[key] = str(value).strip()


_apply_runtime_secrets()

st.set_page_config(
    page_title="CanAdapt — vagas no Canadá",
    page_icon="🍁",
    layout="wide",
)

pg = st.navigation(
    [
        st.Page(
            "app_pages/apresentacao.py",
            title="Apresentação",
            icon=":material/home:",
            default=True,
        ),
        st.Page(
            "app_pages/vagas.py",
            title="Explorar vagas",
            icon=":material/work:",
            url_path="vagas",
        ),
    ],
    position="top",
)
pg.run()

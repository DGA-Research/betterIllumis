import io
import tempfile
import zipfile
from pathlib import Path

import pandas as pd
import streamlit as st

from generate_kristin_robbins_votes import (
    WORKBOOK_HEADERS,
    collect_legislator_names,
    collect_vote_rows,
    write_workbook,
)


def _collect_legislators_from_zip(zip_bytes: bytes):
    with tempfile.TemporaryDirectory() as tmp_dir:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            zf.extractall(tmp_dir)
        return collect_legislator_names(Path(tmp_dir))


def _generate_export_from_zip(zip_bytes: bytes, legislator_name: str):
    with tempfile.TemporaryDirectory() as tmp_dir:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            zf.extractall(tmp_dir)
        base_dir = Path(tmp_dir)
        rows = collect_vote_rows(base_dir, legislator_name)
        buffer = io.BytesIO()
        write_workbook(rows, legislator_name, buffer)
        buffer.seek(0)
        return rows, buffer


def _make_download_filename(name: str) -> str:
    slug = "_".join(part for part in name.lower().split() if part)
    return f"{slug or 'legislator'}_votes.xlsx"


st.set_page_config(page_title="LegiScan Vote Explorer", layout="wide")
st.title("LegiScan Vote Explorer")
st.caption(
    "Upload a LegiScan ZIP archive for a single state and session range, then choose a legislator to generate a vote summary."
)

uploaded_zip = st.file_uploader("LegiScan ZIP", type="zip")

if uploaded_zip is None:
    st.info("Upload a ZIP file to get started.")
    st.stop()

try:
    zip_bytes = uploaded_zip.getvalue()
except Exception as exc:  # pragma: no cover - streamlit runtime guard
    st.error(f"Failed to read uploaded file: {exc}")
    st.stop()

try:
    legislator_options = _collect_legislators_from_zip(zip_bytes)
except zipfile.BadZipFile:
    st.error("The uploaded file is not a valid ZIP archive.")
    st.stop()
except FileNotFoundError as exc:
    st.error(f"{exc}")
    st.stop()

if not legislator_options:
    st.warning("No legislators found in the uploaded dataset.")
    st.stop()

selected_legislator = st.selectbox("Legislator", legislator_options)

if not selected_legislator:
    st.stop()

if st.button("Generate vote summary"):
    with st.spinner("Processing LegiScan data..."):
        try:
            rows, workbook_buffer = _generate_export_from_zip(
                zip_bytes, selected_legislator
            )
        except ValueError as exc:
            st.warning(str(exc))
            st.stop()

    st.success(f"Compiled {len(rows)} votes for {selected_legislator}.")

    st.download_button(
        label="Download Excel workbook",
        data=workbook_buffer.getvalue(),
        file_name=_make_download_filename(selected_legislator),
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.subheader("Vote Breakdown")
    summary_df = pd.DataFrame(rows, columns=WORKBOOK_HEADERS)
    st.dataframe(summary_df, use_container_width=True, height=600)

import io
import tempfile
import zipfile
from contextlib import ExitStack
from pathlib import Path
from typing import List

import pandas as pd
import streamlit as st

from generate_kristin_robbins_votes import (
    WORKBOOK_HEADERS,
    collect_legislator_names,
    collect_vote_rows,
    determine_dataset_state,
    write_workbook,
)


def _collect_legislators_from_zips(zip_payloads: List[bytes]):
    with ExitStack() as stack:
        base_dirs = []
        for payload in zip_payloads:
            temp_dir = stack.enter_context(tempfile.TemporaryDirectory())
            with zipfile.ZipFile(io.BytesIO(payload)) as zf:
                zf.extractall(temp_dir)
            base_dirs.append(Path(temp_dir))
        state = determine_dataset_state(base_dirs)
        names = collect_legislator_names(base_dirs)
        return state, names


def _generate_export_from_zips(zip_payloads: List[bytes], legislator_name: str):
    with ExitStack() as stack:
        base_dirs = []
        for payload in zip_payloads:
            temp_dir = stack.enter_context(tempfile.TemporaryDirectory())
            with zipfile.ZipFile(io.BytesIO(payload)) as zf:
                zf.extractall(temp_dir)
            base_dirs.append(Path(temp_dir))
        rows = collect_vote_rows(base_dirs, legislator_name)
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
    "Upload one or more LegiScan ZIP archives from the same state, then choose a legislator to generate a consolidated vote summary."
)

uploaded_zips = st.file_uploader(
    "LegiScan ZIP file(s)", type="zip", accept_multiple_files=True
)

if not uploaded_zips:
    st.info("Upload one or more ZIP files to get started.")
    st.stop()

zip_payloads: List[bytes] = []
for uploaded_zip in uploaded_zips:
    try:
        zip_payloads.append(uploaded_zip.getvalue())
    except Exception as exc:  # pragma: no cover - streamlit runtime guard
        st.error(f"Failed to read uploaded file '{uploaded_zip.name}': {exc}")
        st.stop()

if not zip_payloads:
    st.info("Provide at least one ZIP archive to continue.")
    st.stop()

try:
    dataset_state, legislator_options = _collect_legislators_from_zips(zip_payloads)
except zipfile.BadZipFile:
    st.error("One or more uploads are not valid ZIP archives.")
    st.stop()
except FileNotFoundError as exc:
    st.error(f"{exc}")
    st.stop()
except ValueError as exc:
    st.error(str(exc))
    st.stop()

if not legislator_options:
    st.warning("No legislators found in the uploaded dataset.")
    st.stop()

if dataset_state:
    st.caption(f"Detected state: {dataset_state}")

selected_legislator = st.selectbox("Legislator", legislator_options)

if not selected_legislator:
    st.stop()

if st.button("Generate vote summary"):
    with st.spinner("Processing LegiScan data..."):
        try:
            rows, workbook_buffer = _generate_export_from_zips(
                zip_payloads, selected_legislator
            )
        except zipfile.BadZipFile:
            st.error("One of the uploads could not be read as a ZIP archive.")
            st.stop()
        except ValueError as exc:
            st.warning(str(exc))
            st.stop()

    state_label = f" ({dataset_state})" if dataset_state else ""
    st.success(f"Compiled {len(rows)} votes for {selected_legislator}{state_label}.")

    st.download_button(
        label="Download Excel workbook",
        data=workbook_buffer.getvalue(),
        file_name=_make_download_filename(selected_legislator),
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.subheader("Vote Breakdown")
    summary_df = pd.DataFrame(rows, columns=WORKBOOK_HEADERS)
    st.dataframe(summary_df, use_container_width=True, height=600)

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


def _collect_rows_from_zips(zip_payloads: List[bytes], legislator_name: str):
    with ExitStack() as stack:
        base_dirs = []
        for payload in zip_payloads:
            temp_dir = stack.enter_context(tempfile.TemporaryDirectory())
            with zipfile.ZipFile(io.BytesIO(payload)) as zf:
                zf.extractall(temp_dir)
            base_dirs.append(Path(temp_dir))
        rows = collect_vote_rows(base_dirs, legislator_name)
        return rows


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

with st.sidebar:
    st.header("Filters")
    filter_mode = st.selectbox(
        "Filter preset",
        options=["All Votes", "Votes Against Party", "Minority Votes"],
        index=0,
        help="Choose a predefined view of the legislator's voting record.",
    )

    if filter_mode in {"Votes Against Party", "Minority Votes"}:
        minority_percent = st.slider(
            "Minority threshold (%)",
            min_value=0,
            max_value=100,
            value=20,
            help="Keep votes where the aligned group supporting the legislator's position is at or below this percentage.",
        )
        min_group_votes = st.slider(
            "Minimum votes in group",
            min_value=0,
            max_value=200,
            value=5,
            help="Ignore vote records where the compared group cast fewer total votes than this threshold.",
        )
        if filter_mode == "Votes Against Party":
            st.caption("Shows votes where the legislator sided with a minority of their party.")
        else:
            st.caption("Shows votes where the legislator sided with a minority of both their party and the full chamber.")
    else:
        minority_percent = 20
        min_group_votes = 0

if st.button("Generate vote summary"):
    with st.spinner("Processing LegiScan data..."):
        try:
            rows = _collect_rows_from_zips(zip_payloads, selected_legislator)
        except zipfile.BadZipFile:
            st.error("One of the uploads could not be read as a ZIP archive.")
            st.stop()
        except ValueError as exc:
            st.warning(str(exc))
            st.stop()

    summary_df = pd.DataFrame(rows, columns=WORKBOOK_HEADERS)

    def safe_int(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def calc_metrics(row: pd.Series):
        bucket = row["Vote Bucket"]
        party = row["Person Party"]
        metrics = {
            "party_bucket_votes": None,
            "party_total_votes": None,
            "party_share": None,
            "chamber_bucket_votes": None,
            "chamber_total_votes": None,
            "chamber_share": None,
        }

        if party:
            party_bucket_col = f"{party}_{bucket}"
            party_total_col = f"{party}_Total"
            party_bucket = safe_int(row.get(party_bucket_col))
            party_total = safe_int(row.get(party_total_col))
            metrics["party_bucket_votes"] = party_bucket
            metrics["party_total_votes"] = party_total
            metrics["party_share"] = (
                party_bucket / party_total if party_total else None
            )

        chamber_bucket = safe_int(row.get(f"Total_{bucket}"))
        chamber_total = safe_int(row.get("Total_Total"))
        metrics["chamber_bucket_votes"] = chamber_bucket
        metrics["chamber_total_votes"] = chamber_total
        metrics["chamber_share"] = (
            chamber_bucket / chamber_total if chamber_total else None
        )
        return pd.Series(metrics)

    metrics_df = summary_df.apply(calc_metrics, axis=1)
    summary_df = pd.concat([summary_df, metrics_df], axis=1)

    apply_party_filter = filter_mode in {"Votes Against Party", "Minority Votes"}
    apply_chamber_filter = filter_mode == "Minority Votes"
    threshold_ratio = (minority_percent / 100.0) if apply_party_filter or apply_chamber_filter else None
    min_votes = min_group_votes if apply_party_filter or apply_chamber_filter else 0

    filters = []
    if apply_party_filter:
        party_condition = (
            summary_df["party_share"].notna()
            & (summary_df["party_total_votes"] >= min_votes)
            & (summary_df["party_share"] <= threshold_ratio)
        )
        filters.append(party_condition)
    if apply_chamber_filter:
        chamber_condition = (
            summary_df["chamber_share"].notna()
            & (summary_df["chamber_total_votes"] >= min_votes)
            & (summary_df["chamber_share"] <= threshold_ratio)
        )
        filters.append(chamber_condition)

    if filters:
        mask = filters[0]
        for condition in filters[1:]:
            mask &= condition
        filtered_df = summary_df[mask].copy()
    else:
        filtered_df = summary_df.copy()

    filtered_count = len(filtered_df)
    total_count = len(summary_df)

    state_label = f" ({dataset_state})" if dataset_state else ""
    st.success(
        f"Compiled {total_count} votes for {selected_legislator}{state_label}. "
        f"Showing {filtered_count} after filters."
    )

    if filtered_count == 0:
        st.warning("No vote records matched the current filters.")

    download_buffer = io.BytesIO()
    write_workbook(
        filtered_df[WORKBOOK_HEADERS].values.tolist(),
        selected_legislator,
        download_buffer,
    )
    download_buffer.seek(0)

    st.download_button(
        label="Download filtered Excel workbook",
        data=download_buffer.getvalue(),
        file_name=_make_download_filename(selected_legislator),
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    display_df = filtered_df.copy()
    date_serials = pd.to_numeric(display_df["Date"], errors="coerce")
    display_df["Date"] = pd.to_datetime("1899-12-30") + pd.to_timedelta(
        date_serials, unit="D"
    )
    display_df["Date"] = display_df["Date"].dt.date
    display_df["Party Minority %"] = (
        display_df["party_share"] * 100
    ).round(1)
    display_df["Chamber Minority %"] = (
        display_df["chamber_share"] * 100
    ).round(1)
    display_df = display_df.rename(
        columns={
            "party_bucket_votes": "Party Votes (same position)",
            "party_total_votes": "Party Total Votes",
            "chamber_bucket_votes": "Chamber Votes (same position)",
            "chamber_total_votes": "Chamber Total Votes",
        }
    )

    st.subheader("Vote Breakdown")
    st.dataframe(
        display_df.drop(
            columns=[
                "party_share",
                "chamber_share",
            ]
        ),
        use_container_width=True,
        height=600,
    )

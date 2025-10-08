import csv
import datetime as dt
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
    gather_session_csv_dirs,
    collect_person_vote_map,
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


def _collect_person_votes_from_zips(zip_payloads: List[bytes], legislator_name: str):
    with ExitStack() as stack:
        base_dirs = []
        for payload in zip_payloads:
            temp_dir = stack.enter_context(tempfile.TemporaryDirectory())
            with zipfile.ZipFile(io.BytesIO(payload)) as zf:
                zf.extractall(temp_dir)
            base_dirs.append(Path(temp_dir))
        return collect_person_vote_map(base_dirs, legislator_name)


def _collect_years_from_zips(zip_payloads: List[bytes]):
    years = set()
    with ExitStack() as stack:
        base_dirs = []
        for payload in zip_payloads:
            temp_dir = stack.enter_context(tempfile.TemporaryDirectory())
            with zipfile.ZipFile(io.BytesIO(payload)) as zf:
                zf.extractall(temp_dir)
            base_dirs.append(Path(temp_dir))
        try:
            csv_dirs = gather_session_csv_dirs(base_dirs)
        except FileNotFoundError:
            return []
        for csv_dir in csv_dirs:
            rollcalls_path = csv_dir / "rollcalls.csv"
            if not rollcalls_path.exists():
                continue
            with rollcalls_path.open(encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    date_str = (row.get("date") or "").strip()
                    if not date_str:
                        continue
                    try:
                        year = dt.datetime.strptime(date_str, "%Y-%m-%d").year
                    except ValueError:
                        continue
                    years.add(year)
    return sorted(years)


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

year_options = _collect_years_from_zips(zip_payloads)

comparison_person = None
comparison_label = ""
max_vote_diff = 5

with st.sidebar:
    st.header("Filters")
    filter_mode = st.selectbox(
        "Vote type",
        options=[
            "All Votes",
            "Votes Against Party",
            "Votes With Person",
            "Votes Against Person",
            "Minority Votes",
            "Deciding Votes",
            "Skipped Votes",
        ],
        index=0,
        help="Choose a predefined view of the legislator's voting record.",
    )

    party_focus_option = "Legislator's Party"
    if filter_mode == "All Votes":
        minority_percent = 20
        min_group_votes = 0
    elif filter_mode == "Votes Against Party":
        party_focus_option = st.selectbox(
            "Party voting against",
            options=["Legislator's Party", "Democrat", "Republican", "Independent"],
            index=0,
            help="Choose which party's vote breakdown to compare against.",
        )
        minority_percent = st.slider(
            "Minority threshold (%)",
            min_value=0,
            max_value=100,
            value=20,
            help="Keep votes where the selected party supported the legislator's position at or below this percentage.",
        )
        min_group_votes = st.slider(
            "Minimum votes in group",
            min_value=0,
            max_value=200,
            value=5,
            help="Ignore vote records where the compared party cast fewer total votes than this threshold.",
        )
        st.caption("Shows votes where the legislator sided with a minority of the chosen party.")
    elif filter_mode == "Votes With Person":
        comparison_label = "Person voting with"
        comparison_person = st.selectbox(
            comparison_label,
            options=legislator_options,
            index=0,
            help="Select another legislator to find votes where they aligned.",
        )
        minority_percent = 20
        min_group_votes = 0
        st.caption("Shows votes where the legislator and selected colleague cast the same vote.")
    elif filter_mode == "Votes Against Person":
        comparison_label = "Person voting against"
        comparison_person = st.selectbox(
            comparison_label,
            options=legislator_options,
            index=0,
            help="Select another legislator to find votes where their positions opposed each other.",
        )
        minority_percent = 20
        min_group_votes = 0
        st.caption("Shows votes where the legislator and selected colleague took opposing sides.")
    elif filter_mode == "Minority Votes":
        minority_percent = st.slider(
            "Minority threshold (%)",
            min_value=0,
            max_value=100,
            value=20,
            help="Keep votes where the legislator's party supported their position at or below this percentage.",
        )
        min_group_votes = st.slider(
            "Minimum votes in group",
            min_value=0,
            max_value=200,
            value=5,
            help="Ignore vote records where the compared group cast fewer total votes than this threshold.",
        )
        st.caption("Shows votes where the legislator sided with a minority of both their party and the full chamber.")
    elif filter_mode == "Deciding Votes":
        minority_percent = 20
        min_group_votes = 0
        max_vote_diff = st.slider(
            "Maximum votes difference",
            min_value=1,
            max_value=50,
            value=5,
            help="Limit to votes where the margin between Yeas and Nays is within this amount.",
        )
        st.caption("Shows votes where the legislator's side prevailed by the specified margin or less.")
    else:  # Skipped Votes
        minority_percent = 20
        min_group_votes = 0
        st.caption("Shows votes where the legislator did not cast a Yea or Nay.")

    st.subheader("Year")
    year_selection = st.multiselect(
        "Year",
        options=year_options,
        default=year_options,
        help="Restrict votes to selected calendar years.",
    )

    st.divider()
    st.header("Legislator")
    selected_legislator = st.selectbox("Legislator", legislator_options)

if not selected_legislator:
    st.stop()

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

    date_serials = pd.to_numeric(summary_df["Date"], errors="coerce")
    summary_df["Date_dt"] = pd.to_datetime("1899-12-30") + pd.to_timedelta(
        date_serials, unit="D"
    )
    summary_df["Year"] = summary_df["Date_dt"].dt.year

    if year_selection:
        summary_df = summary_df[summary_df["Year"].isin(year_selection)].copy()

    if summary_df.empty:
        st.warning("No vote records found for the selected criteria.")
        st.stop()

    if filter_mode == "Skipped Votes":
        vote_text = summary_df["Vote"].astype(str).str.strip().str.lower()
        skip_mask = ~(
            vote_text.str.startswith("yea")
            | vote_text.str.startswith("nay")
            | vote_text.str.startswith("aye")
        )
        summary_df = summary_df[skip_mask].copy()
        if summary_df.empty:
            st.warning("No skipped votes found for the selected criteria.")
            st.stop()

    summary_df["Roll Call ID"] = pd.to_numeric(
        summary_df["Roll Call ID"], errors="coerce"
    ).astype("Int64")

    if filter_mode in {"Votes With Person", "Votes Against Person"}:
        if not comparison_person:
            st.warning("Select a comparison legislator in the sidebar.")
            st.stop()
        if comparison_person == selected_legislator:
            st.warning("Choose a different legislator for comparison.")
            st.stop()

        comparison_votes = _collect_person_votes_from_zips(
            zip_payloads, comparison_person
        )
        if not comparison_votes:
            st.warning(f"No vote records found for {comparison_person}.")
            st.stop()

        def lookup_comparison(rcid):
            if pd.isna(rcid):
                return pd.Series({"Comparison Vote": "", "Comparison Vote Bucket": ""})
            info = comparison_votes.get(int(rcid))
            if not info:
                return pd.Series({"Comparison Vote": "", "Comparison Vote Bucket": ""})
            return pd.Series(
                {
                    "Comparison Vote": info.get("vote_desc", ""),
                    "Comparison Vote Bucket": info.get("vote_bucket", ""),
                }
            )

        comparison_df = summary_df["Roll Call ID"].apply(lookup_comparison)
        summary_df = pd.concat([summary_df, comparison_df], axis=1)

        summary_df = summary_df[summary_df["Comparison Vote Bucket"] != ""].copy()
        if summary_df.empty:
            st.warning(
                f"{comparison_person} has no recorded votes overlapping with {selected_legislator}."
            )
            st.stop()

        main_bucket = summary_df["Vote Bucket"]
        comp_bucket = summary_df["Comparison Vote Bucket"]

        if filter_mode == "Votes With Person":
            comparison_mask = main_bucket == comp_bucket
        else:
            comparison_mask = (
                (main_bucket == "For") & (comp_bucket == "Against")
            ) | ((main_bucket == "Against") & (comp_bucket == "For"))

        summary_df = summary_df[comparison_mask].copy()
        if summary_df.empty:
            verb = "with" if filter_mode == "Votes With Person" else "against"
            st.warning(
                f"No votes found where {selected_legislator} voted {verb} {comparison_person}."
            )
            st.stop()

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

    party_display_map = {
        "Democrat": "Democrat",
        "Republican": "Republican",
        "Other": "Independent",
    }
    focus_party_lookup = {
        "Legislator's Party": None,
        "Democrat": "Democrat",
        "Republican": "Republican",
        "Independent": "Other",
    }

    summary_df["Person Party Display"] = summary_df["Person Party"].map(
        party_display_map
    ).fillna(summary_df["Person Party"])
    summary_df["focus_party_label"] = summary_df["Person Party Display"]
    summary_df["focus_party_bucket_votes"] = summary_df["party_bucket_votes"]
    summary_df["focus_party_total_votes"] = summary_df["party_total_votes"]
    summary_df["focus_party_share"] = summary_df["party_share"]

    focus_party_key = focus_party_lookup.get(party_focus_option)
    if filter_mode == "Votes Against Party" and focus_party_key:
        focus_display_label = (
            "Independent" if focus_party_key == "Other" else party_focus_option
        )

        def calc_focus_metrics(row: pd.Series):
            bucket = row["Vote Bucket"]
            bucket_votes = safe_int(row.get(f"{focus_party_key}_{bucket}"))
            total_votes = safe_int(row.get(f"{focus_party_key}_Total"))
            share = bucket_votes / total_votes if total_votes else None
            return pd.Series(
                {
                    "focus_party_label": focus_display_label,
                    "focus_party_bucket_votes": bucket_votes,
                    "focus_party_total_votes": total_votes,
                    "focus_party_share": share,
                }
            )

        focus_metrics = summary_df.apply(calc_focus_metrics, axis=1)
        summary_df[
            [
                "focus_party_label",
                "focus_party_bucket_votes",
                "focus_party_total_votes",
                "focus_party_share",
            ]
        ] = focus_metrics

    deciding_condition = None
    if filter_mode == "Deciding Votes":
        total_for = summary_df["Total_For"].apply(safe_int)
        total_against = summary_df["Total_Against"].apply(safe_int)
        vote_diff = (total_for - total_against).abs()
        winning_bucket = pd.Series(
            "Tie", index=summary_df.index, dtype="object"
        )
        winning_bucket = winning_bucket.mask(total_for > total_against, "For")
        winning_bucket = winning_bucket.mask(total_against > total_for, "Against")
        summary_df["Vote Difference"] = vote_diff
        summary_df["Winning Bucket"] = winning_bucket
        deciding_condition = (
            (vote_diff <= max_vote_diff)
            & winning_bucket.isin(["For", "Against"])
            & (summary_df["Vote Bucket"] == winning_bucket)
        )

    apply_party_filter = filter_mode in {"Votes Against Party", "Minority Votes"}
    apply_chamber_filter = filter_mode == "Minority Votes"
    threshold_ratio = (
        minority_percent / 100.0 if (apply_party_filter or apply_chamber_filter) else None
    )
    min_votes = min_group_votes if (apply_party_filter or apply_chamber_filter) else 0

    filters = []
    if apply_party_filter:
        party_condition = (
            summary_df["focus_party_share"].notna()
            & (summary_df["focus_party_total_votes"] >= min_votes)
            & (summary_df["focus_party_share"] <= threshold_ratio)
        )
        filters.append(party_condition)
    if apply_chamber_filter:
        chamber_condition = (
            summary_df["chamber_share"].notna()
            & (summary_df["chamber_total_votes"] >= min_votes)
            & (summary_df["chamber_share"] <= threshold_ratio)
        )
        filters.append(chamber_condition)
    if filter_mode == "Deciding Votes" and deciding_condition is not None:
        filters.append(deciding_condition)

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
    display_df["Date"] = display_df["Date_dt"].dt.date
    display_df["Legislator Party"] = display_df["Person Party"].map(
        party_display_map
    ).fillna(display_df["Person Party"])
    display_df["Focus Party"] = display_df["focus_party_label"]
    if filter_mode in {"Votes With Person", "Votes Against Person"}:
        display_df["Comparison Legislator"] = comparison_person
    display_df["Legislator Party Minority %"] = (
        display_df["party_share"] * 100
    ).round(1)
    display_df["Focus Party Minority %"] = (
        display_df["focus_party_share"] * 100
    ).round(1)
    display_df["Chamber Minority %"] = (
        display_df["chamber_share"] * 100
    ).round(1)
    display_df = display_df.rename(
        columns={
            "party_bucket_votes": "Legislator Party Votes (same position)",
            "party_total_votes": "Legislator Party Total Votes",
            "focus_party_bucket_votes": "Focus Party Votes (same position)",
            "focus_party_total_votes": "Focus Party Total Votes",
            "chamber_bucket_votes": "Chamber Votes (same position)",
            "chamber_total_votes": "Chamber Total Votes",
            "Vote Difference": "Vote Margin",
            "Winning Bucket": "Winning Side",
        }
    )

    st.subheader("Vote Breakdown")
    st.dataframe(
        display_df.drop(
            columns=[
                "party_share",
                "focus_party_share",
                "chamber_share",
                "focus_party_label",
                "Person Party Display",
                "Date_dt",
            ],
            errors="ignore",
        ),
        use_container_width=True,
        height=600,
    )

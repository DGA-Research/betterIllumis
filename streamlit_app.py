import csv
import datetime as dt
import io
import tempfile
import zipfile
from contextlib import ExitStack
from pathlib import Path
from typing import List, Optional

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

LOCAL_ARCHIVE_DIR = Path(__file__).resolve().parent / "bulkLegiData"
BUNDLED_ARCHIVE_SESSION_KEY = "bundled_archive_selection"
ALL_STATES_LABEL = "All States"
STATE_CHOICES = [
    ("Alabama", "AL"),
    ("Alaska", "AK"),
    ("Arizona", "AZ"),
    ("Arkansas", "AR"),
    ("California", "CA"),
    ("Colorado", "CO"),
    ("Connecticut", "CT"),
    ("Delaware", "DE"),
    ("Florida", "FL"),
    ("Georgia", "GA"),
    ("Hawaii", "HI"),
    ("Idaho", "ID"),
    ("Illinois", "IL"),
    ("Indiana", "IN"),
    ("Iowa", "IA"),
    ("Kansas", "KS"),
    ("Kentucky", "KY"),
    ("Louisiana", "LA"),
    ("Maine", "ME"),
    ("Maryland", "MD"),
    ("Massachusetts", "MA"),
    ("Michigan", "MI"),
    ("Minnesota", "MN"),
    ("Mississippi", "MS"),
    ("Missouri", "MO"),
    ("Montana", "MT"),
    ("Nebraska", "NE"),
    ("Nevada", "NV"),
    ("New Hampshire", "NH"),
    ("New Jersey", "NJ"),
    ("New Mexico", "NM"),
    ("New York", "NY"),
    ("North Carolina", "NC"),
    ("North Dakota", "ND"),
    ("Ohio", "OH"),
    ("Oklahoma", "OK"),
    ("Oregon", "OR"),
    ("Pennsylvania", "PA"),
    ("Rhode Island", "RI"),
    ("South Carolina", "SC"),
    ("South Dakota", "SD"),
    ("Tennessee", "TN"),
    ("Texas", "TX"),
    ("Utah", "UT"),
    ("Vermont", "VT"),
    ("Virginia", "VA"),
    ("Washington", "WA"),
    ("West Virginia", "WV"),
    ("Wisconsin", "WI"),
    ("Wyoming", "WY"),
]
STATE_NAME_TO_CODE = {name: code for name, code in STATE_CHOICES}


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


def _list_local_archives() -> List[Path]:
    if not LOCAL_ARCHIVE_DIR.exists():
        return []
    return sorted(
        path for path in LOCAL_ARCHIVE_DIR.glob("*.zip") if path.is_file()
    )


def _archive_matches_state(name: str, state_code: str) -> bool:
    if not state_code:
        return True
    prefix = state_code.upper()
    normalized = name.upper()
    return normalized.startswith(prefix)


def _collect_latest_action_date(zip_payloads: List[bytes]) -> Optional[dt.date]:
    latest_date: Optional[dt.date] = None
    with ExitStack() as stack:
        for payload in zip_payloads:
            temp_dir = stack.enter_context(tempfile.TemporaryDirectory())
            with zipfile.ZipFile(io.BytesIO(payload)) as zf:
                zf.extractall(temp_dir)
            base_dir = Path(temp_dir)
            try:
                csv_dirs = gather_session_csv_dirs([base_dir])
            except FileNotFoundError:
                continue
            for csv_dir in csv_dirs:
                bills_path = csv_dir / "bills.csv"
                if not bills_path.exists():
                    continue
                with bills_path.open(encoding="utf-8") as fh:
                    reader = csv.DictReader(fh)
                    for row in reader:
                        date_str = (row.get("last_action_date") or "").strip()
                        if not date_str:
                            continue
                        try:
                            parsed_date = dt.datetime.strptime(
                                date_str, "%Y-%m-%d"
                            ).date()
                        except ValueError:
                            continue
                        if latest_date is None or parsed_date > latest_date:
                            latest_date = parsed_date
    return latest_date


def _render_state_filter():
    st.sidebar.header("Data Source")
    state_query = st.sidebar.text_input(
        "State search",
        value="",
        placeholder="Type to filter states",
        help="Filter the list of states shown below.",
    )
    normalized_query = state_query.strip().lower()
    if normalized_query:
        filtered_states = [
            name
            for name, code in STATE_CHOICES
            if normalized_query in name.lower() or normalized_query in code.lower()
        ]
    else:
        filtered_states = [name for name, _ in STATE_CHOICES]

    if not filtered_states:
        st.sidebar.info("No states match that search. Showing all states.")
        filtered_states = [name for name, _ in STATE_CHOICES]

    state_label = st.sidebar.selectbox(
        "State",
        options=[ALL_STATES_LABEL] + filtered_states,
        index=0,
        key="state_filter_select",
        help="Filter archives by the state's two-letter prefix (e.g., MN_...).",
    )
    return state_label, STATE_NAME_TO_CODE.get(state_label)


st.set_page_config(page_title="LegiScan Vote Explorer", layout="wide")
st.title("LegiScan Vote Explorer")
st.caption(
    "Upload one or more LegiScan ZIP archives from the same state, then choose a legislator to generate a consolidated vote summary."
)

state_label, state_code = _render_state_filter()

uploaded_zips = st.file_uploader(
    "LegiScan ZIP file(s)", type="zip", accept_multiple_files=True
)

all_local_archive_paths = _list_local_archives()
local_archive_paths = [
    path
    for path in all_local_archive_paths
    if _archive_matches_state(path.name, state_code)
] if state_code else all_local_archive_paths
selected_local_archives: List[Path] = []
if local_archive_paths:
    local_lookup = {path.name: path for path in local_archive_paths}
    available_names = list(local_lookup.keys())
    existing_selection = st.session_state.get(BUNDLED_ARCHIVE_SESSION_KEY, [])
    filtered_selection = [name for name in existing_selection if name in local_lookup]
    if filtered_selection != existing_selection:
        st.session_state[BUNDLED_ARCHIVE_SESSION_KEY] = filtered_selection
    elif BUNDLED_ARCHIVE_SESSION_KEY not in st.session_state:
        st.session_state[BUNDLED_ARCHIVE_SESSION_KEY] = []

    select_all_col, clear_col = st.columns([1, 1])
    with select_all_col:
        if st.button("Select all bundled", use_container_width=True):
            st.session_state[BUNDLED_ARCHIVE_SESSION_KEY] = available_names
    with clear_col:
        if st.button("Clear bundled", use_container_width=True):
            st.session_state[BUNDLED_ARCHIVE_SESSION_KEY] = []

    selected_local_names = st.multiselect(
        "Bundled LegiScan archive(s)",
        options=available_names,
        key=BUNDLED_ARCHIVE_SESSION_KEY,
        help="Include ZIP archives stored in the repository (bulkLegiData).",
    )
    selected_local_archives = [
        local_lookup[name] for name in selected_local_names
    ]
else:
    if state_code and all_local_archive_paths:
        st.caption(
            f"No bundled archives match the selected state ({state_label})."
        )
    elif not uploaded_zips:
        st.caption(
            "Add additional archives under the 'bulkLegiData' directory to make them selectable here."
        )

if not uploaded_zips and not selected_local_archives:
    st.info("Upload ZIP files or select bundled archives to get started.")
    st.stop()

zip_payloads: List[bytes] = []
skipped_uploads: List[str] = []
for uploaded_zip in uploaded_zips or []:
    if state_code and not _archive_matches_state(uploaded_zip.name, state_code):
        skipped_uploads.append(uploaded_zip.name)
        continue
    try:
        zip_payloads.append(uploaded_zip.getvalue())
    except Exception as exc:  # pragma: no cover - streamlit runtime guard
        st.error(f"Failed to read uploaded file '{uploaded_zip.name}': {exc}")
        st.stop()

for archive_path in selected_local_archives:
    try:
        zip_payloads.append(archive_path.read_bytes())
    except OSError as exc:
        st.error(f"Failed to read bundled archive '{archive_path.name}': {exc}")
        st.stop()

if skipped_uploads:
    st.warning(
        f"Skipped uploads that do not match the selected state ({state_label}): "
        + ", ".join(skipped_uploads)
    )

if not zip_payloads:
    st.info("Provide at least one ZIP archive to continue.")
    st.stop()

latest_action_date: Optional[dt.date] = None
if state_code:
    latest_action_date = _collect_latest_action_date(zip_payloads)
    if latest_action_date:
        st.sidebar.caption(
            f"Latest bill action ({state_label}): "
            f"{latest_action_date.strftime('%B %d, %Y')}"
        )
    else:
        st.sidebar.caption(
            f"No bill action dates found for {state_label} in the selected archives."
        )

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
            "Search By Term",
        ],
        index=0,
        help="Choose a predefined view of the legislator's voting record.",
    )

    party_focus_option = "Legislator's Party"
    search_term = st.text_input(
        "Search term (bill description)",
        value="",
        help="Filter votes whose bill description contains this text (case-insensitive). Leave blank to disable.",
    )

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
    elif filter_mode == "Search By Term":
        minority_percent = 20
        min_group_votes = 0
        st.caption("Shows votes where the bill description matches the search term.")
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

    if search_term:
        description_mask = summary_df["Bill Description"].astype(str).str.contains(
            search_term, case=False, na=False
        )
        summary_df = summary_df[description_mask].copy()

    if summary_df.empty:
        message = "No vote records found for the selected criteria."
        if search_term:
            message = f"No vote records found matching '{search_term}'."
        st.warning(message)
        st.stop()

    if filter_mode == "Search By Term" and not search_term:
        st.warning("Enter a search term to use the 'Search By Term' vote type.")
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

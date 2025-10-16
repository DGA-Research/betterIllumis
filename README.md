# LegiScan Vote Explorer

LegiScan Vote Explorer is a Streamlit application for inspecting how an individual legislator voted across one or more LegiScan bulk downloads. Upload the ZIP archives for a state, pick a legislator, and the app will consolidate every roll call into an interactive table and downloadable Excel workbook.

## Features
- Combine multiple LegiScan session archives from the same state.
- Detect available legislators automatically and filter votes by calendar year.
- Apply targeted views: minority votes, cross-party comparisons, tight vote margins, skipped votes, or keyword search.
- Compare a legislator against another colleague to find aligned or opposing roll calls.
- Export the filtered results as an Excel workbook that mirrors the on-screen table.

## Virtual Access 
1. App can be accessed virtually via: https://betterillumis-ocj7zc4myap2gpcmcrebpb.streamlit.app/

## Local Prerequisites (optional)
- Python 3.9 or newer.
- Pip for installing Python packages.
- LegiScan bulk download ZIP archives that include `bills.csv`, `people.csv`, `rollcalls.csv`, and `votes.csv` within a `.../csv/` folder.

## Local Installation (optional)
1. Clone or download this repository.
2. (Optional) Create and activate a virtual environment:  
   `python -m venv .venv` followed by `.venv\Scripts\activate` (Windows) or `source .venv/bin/activate` (macOS/Linux).
3. Install the required packages:
   ```bash
   pip install streamlit pandas openpyxl
   ```
   *`requirements.txt` pins `openpyxl`; installing the other packages alongside it keeps the app runnable.*

## Locally Run the App (optional)
1. From the project directory, start Streamlit:
   ```bash
   streamlit run streamlit_app.py
   ```
2. Open the displayed local URL (default `http://localhost:8501`) in your browser.

## Preparing Your LegiScan Data
1. Download one or more LegiScan bulk archives (e.g., via the LegiScan Bulk Data service).
2. Ensure each archive retains the original folder layout so that each session folder contains a `csv` directory with `bills.csv`, `people.csv`, `rollcalls.csv`, and `votes.csv`.
3. Only upload archives from a single state at a time. Mixing states causes the app to stop with an error to keep results consistent.

## Using the Interface
1. Upload the LegiScan ZIP file(s). Multiple archives are allowed and will be merged if they describe the same state.
2. Pick a legislator from the detected list.
3. Adjust sidebar options as needed:
   - **Vote type**  
     `All Votes` – full history.  
     `Votes Against Party` – minority votes relative to a chosen party with adjustable threshold and minimum participation.  
     `Votes With Person` / `Votes Against Person` – compare outcomes with another legislator.  
     `Minority Votes` – votes where both the legislator’s party and the full chamber were in the minority.  
     `Deciding Votes` – filters to roll calls decided by a configurable margin.  
     `Skipped Votes` – highlights non-yea/nay records.  
     `Search By Term` – keyword match on bill descriptions.
   - **Search term** – case-insensitive filter on bill descriptions.
   - **Year** – multiselect to restrict the time window.
   - **Party/Person controls** – sliders and dropdowns change based on the selected vote type.
4. Click **Generate vote summary**. The app parses the uploads, applies filters, and shows how many votes remain.
5. Review the interactive table. Columns include vote counts by party, chamber totals, margin details, and any comparison columns you enabled.

## Exporting Results
- Use **Download filtered Excel workbook** to save the current view as `<legislator>_votes.xlsx`.
- The workbook uses the same column order as the on-screen table, making it easy to reuse in spreadsheets or share with stakeholders.

## Troubleshooting
- **Invalid ZIP** – If a file cannot be opened, Streamlit reports which upload failed; re-download the archive and try again.
- **Mixed states detected** – Upload archives from a single state per session; remove any out-of-state files before re-running.
- **No votes found** – Verify the legislator exists in the uploaded sessions and adjust filters (especially keyword search and comparison options).

Happy exploring!

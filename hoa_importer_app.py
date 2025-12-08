import streamlit as st
import pandas as pd
import json
import csv
import re
import requests
from io import StringIO


# ----------------------------------------------------
# QUICKBASE CONFIG
# ----------------------------------------------------
QB_REALM = st.secrets["QB_REALM"]
QB_TABLE_ID = st.secrets["QB_TABLE_ID"]
QB_USER_TOKEN = st.secrets["QB_USER_TOKEN"]


def extract_table_rows(raw_text):
    """
    Extracts valid directory rows from messy UI-heavy text files.
    Accepts files like the user-uploaded example.
    """

    lines = raw_text.splitlines()
    rows = []

    for line in lines:
        # Remove garbage formatting elements
        cleaned = line.replace("\t", " ").strip()

        # Skip empty or irrelevant lines
        if not cleaned:
            continue
        if cleaned.startswith(("Cookie", "Skip to", "I'm interested", "Cart", "Login",
                               "Search", "Keyword", "About", "ad", "UNITED STATES")):
            continue
        
        # Basic detection of a real data row:
        has_email = "@" in cleaned
        has_zip = re.search(r"\b\d{5}(-\d{4})?\b", cleaned)
        has_phone = re.search(r"\(\d{3}\)\s*\d{3}-\d{4}", cleaned)

        if has_email and has_zip:
            # Likely a valid row → split by two or more spaces
            parts = re.split(r"\s{2,}", cleaned)
            rows.append(parts)

    return rows


def parse_address(full_address):
    if not full_address:
        return "", "", "", ""

    addr = full_address.replace(",", "").strip()
    parts = addr.split()

    # Remove trailing country identifiers
    if len(parts) >= 2 and parts[-2].upper() in ["UNITED", "USA"] and parts[-1].upper() in ["STATES", "AMERICA"]:
        parts = parts[:-2]

    if len(parts) < 4:
        return full_address, "", "", ""

    zipcode = parts[-1]
    state = parts[-2]
    city = parts[-3]
    street = " ".join(parts[:-3])
    return street, city, state, zipcode


# ----------------------------------------------------
# CLEAN UNITS (safe integer)
# ----------------------------------------------------
def clean_units(value):
    if not value:
        return 0
    digits = re.sub(r"[^\d]", "", str(value))
    return int(digits) if digits else 0



# ----------------------------------------------------
# MAIN ROW PARSER (Format B)
# ----------------------------------------------------

def parse_row(parts):
    parts = [p.strip() for p in parts]

    # Skip junk rows early
    txt = " ".join(parts).lower()
    if len(parts) < 6 or "```" in txt or "..." in txt or "there are" in txt:
        return None

    # Fix: collapse extra columns into Format B length
    if len(parts) > 11:
        core = parts[:11]
        return parse_row(core)

    # ----------------------------------------------------
    # FORMAT B — 11 columns (ideal)
    # ----------------------------------------------------
    if len(parts) == 11:
        return {
            "Company": parts[0],
            "Property": parts[1],
            "Street": parts[2],
            "City": parts[3],
            "State": parts[4],
            "Zip": parts[5],
            "Phone": parts[6],
            "Email": parts[7],
            "Units": parts[8],
            "Association": parts[9] or "HAA",
            "Member Type": parts[10] or "Owner",
        }

    # ----------------------------------------------------
    # FORMAT A — 6 columns
    # ----------------------------------------------------
    if len(parts) == 6:
        street, city, state, zipcode = parse_address(parts[2])
        return {
            "Company": parts[0],
            "Property": parts[1],
            "Street": street,
            "City": city,
            "State": state,
            "Zip": zipcode,
            "Phone": parts[3],
            "Email": parts[4],
            "Units": parts[5],
            "Association": "HAA",
            "Member Type": "Owner",
        }

    # ----------------------------------------------------
    # FORMAT C — 7 columns
    # ----------------------------------------------------
    if len(parts) == 7:
        merged = f"{parts[2]} {parts[3]}"
        street, city, state, zipcode = parse_address(merged)
        return {
            "Company": parts[0],
            "Property": parts[1],
            "Street": street,
            "City": city,
            "State": state,
            "Zip": zipcode,
            "Phone": parts[4],
            "Email": parts[5],
            "Units": parts[6],
            "Association": "HAA",
            "Member Type": "Owner",
        }

    return None



# ----------------------------------------------------
# QUICKBASE INSERT FUNCTION
# ----------------------------------------------------
def send_to_quickbase(df: pd.DataFrame):
    url = "https://api.quickbase.com/v1/records"
    headers = {
        "QB-Realm-Hostname": QB_REALM,
        "Authorization": f"QB-USER-TOKEN {QB_USER_TOKEN}",
        "Content-Type": "application/json",
    }

    results = []

    for _, row in df.iterrows():
        units_clean = clean_units(row["Units"])


        record = {
            "6":  {"value": row["Association"]},
            "7":  {"value": row["Property"]},
            "8":  {"value": row["Member Type"]},
            "9":  {"value": f"{row['Street']} {row['City']} {row['State']} {row['Zip']}"},
            "10": {"value": row["City"]},
            "11": {"value": row["State"]},
            "12": {"value": row["Zip"]},
            "14": {"value": row["Phone"]},
            "15": {"value": row["Email"]},
            "18": {"value": row["Company"]},
            "22": {"value": units_clean},
        }

        payload = {"to": QB_TABLE_ID, "data": [record]}
        resp = requests.post(url, headers=headers, json=payload)

        results.append({
            "Property": row["Property"],
            "Status": resp.status_code,
            "Response": resp.text
        })

    return pd.DataFrame(results)


# ----------------------------------------------------
# STREAMLIT UI
# ----------------------------------------------------
st.title("🏢 HOA Directory → Quickbase Import Tool")
st.write("Upload a `.txt` or `.csv` directory export, and this tool will normalize it into a clean dataset ready for Quickbase.")

uploaded_file = st.file_uploader("Upload HOA Directory File", type=["txt", "csv"])

if uploaded_file:
    raw_text = uploaded_file.read().decode("utf-8")

    # Step 1: Extract possible table rows from messy text
    detected_rows = extract_table_rows(raw_text)

    rows = []

    # Step 2: Parse each detected row
    for parts in detected_rows:
        parsed = parse_row(parts)
        if parsed:
            rows.append(parsed)

    # Step 3: Validate results
    if rows:
        df = pd.DataFrame(rows)
        st.success(f"Parsed {len(df)} valid rows!")
        st.dataframe(df, use_container_width=True)

        # Download CSV
        csv_data = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇ Download Cleaned CSV",
            csv_data,
            "hoa_cleaned.csv",
            "text/csv"
        )

        # Send to Quickbase
        if st.button("📤 Import to Quickbase"):
            results_df = send_to_quickbase(df)
            st.write("### Quickbase Import Results")
            st.dataframe(results_df)

    else:
        st.error("No valid rows were detected in this file.")
  

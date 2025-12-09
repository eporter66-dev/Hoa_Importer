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


def detect_association(raw_text):
    """
    Detect apartment/IFMA/CAI/BOMA/APPA associations from uploaded text.
    Returns a standardized code.
    """

    text = raw_text.upper()

    mapping = {
        # Apartment Associations
        "APARTMENT ASSOCIATION OF GREATER ORLANDO": "AAGO",
        "FIRST COAST AA": "FCAA",
        "FIRST COAST APARTMENT ASSOCIATION": "FCAA",
        "CAPITAL CITY AA": "CCAA",
        "NORTHWEST FLORIDA APARTMENT ASSOC": "NWFAA",
        "MOBILE BAY AREA APARTMENT ASSOC": "MBAAA",
        "GREATER GULF COAST APARTMENT ASSOCIATION": "GGCAA",
        "BATON ROUGE APARTMENT ASSOCIATION": "BRAA",
        "SHREVEPORT/BOSSIER AA": "SBAA",
        "ACADIANA APARTMENT ASSOCATION": "AAA",
        "HOUSTON APARTMENT ASSOCIATION": "HAA",
        "APARTMENT ASSOCIATION OF GREATER MEMPHIS": "AAGM",
        "GREATER NASHVILLE APARTMENT ASSOCIATION": "GNAA",
        "UPPER STATE APARTMENT ASSOC": "USAA",
        "GREATER COLUMBUS APARTMENT ASSOCIATION": "GCAA",

        # IFMA
        "GREATER ORLANDO CHAPTER OF IFMA": "IFMA-ORL",
        "IFMA - JACKSONVILLE": "IFMA-JAX",
        "SUNCOAST CHAPTER": "IFMA-SC",
        "CENTRAL AL AND GULF COAST CHAPTER OF IFMA": "IFMA-ALGC",
        "NEW ORLEANS CHAPTER OF IFMA": "IFMA-NO",
        "BATON ROUGE CHAPTER OF IFMA": "IFMA-BR",
        "MEMPHIS CHAPTER OF IFMA": "IFMA-MEM",
        "NASHVILLE CHAPTER OF IFMA": "IFMA-NSH",

        # CAI
        "CAI NORTHEAST FLORIDA": "CAI-NEFL",
        "NORTH GULF COAST CHAPTER": "CAI-NGC",
        "LOUSIANA CHAPTER": "CAI-LA",
        "TENNESSEE CHAPTER": "CAI-TN",

        # BOMA
        "HOUSTON BOMA": "BOMA-HOU",
        "BOMA HOUSTON": "BOMA-HOU",
        "BOMA NASHVILLE": "BOMA-NSH",

        # APPA
        "TAPPA": "TAPPA",
        "MSAPPA": "MSAPPA",
        "FLAPPA": "FLAPPA",

        # TNLA
        "TNLA": "TNLA",
    }

    for key, code in mapping.items():
        if key in text:
            return code

    return "UNKNOWN"



def extract_table_rows(raw_text):
    """
    Extract structured rows from HAA directory exports.
    Works even when pasted as messy text.
    Uses the presence of emails as row anchors.
    """

    lines = raw_text.splitlines()
    rows = []

    email_regex = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"

    for line in lines:
        cleaned = line.strip()

        # Skip obvious non-data lines
        if not cleaned or len(cleaned) < 10:
            continue
        if cleaned.lower().startswith(("cookie", "skip to", "want to find", "search for", "company name", "units greater", "company\tfull", "company full", "to view complete")):
            continue

        # Email determines row boundaries
        email_match = re.search(email_regex, cleaned)
        if not email_match:
            continue

        email = email_match.group(0)

        # Split around email
        before = cleaned[:email_match.start()].strip()
        after = cleaned[email_match.end():].strip()

        # Units should be last
        units = re.sub(r"[^\d]", "", after) if after else ""

        # Before email should contain 4 columns:
        # Company, Full Name, Address, Phone
        parts = re.split(r"\s{2,}|\t", before)

        # If too few parts, fallback to single-space heuristics
        if len(parts) < 4:
            tokens = before.split()
            # last token is phone, address is variable length
            phone_idx = None
            for i, tok in enumerate(tokens):
                if re.match(r"\(?\d{3}\)?[-.\s]*\d{3}[-.\s]*\d{4}", tok):
                    phone_idx = i
                    break

            if phone_idx:
                company = tokens[0]
                full_name = tokens[1]
                address = " ".join(tokens[2:phone_idx])
                phone = tokens[phone_idx]
                parts = [company, full_name, address, phone]

        if len(parts) < 4:
            continue  # skip invalid row

        company = parts[0]
        full_name = parts[1]
        address = parts[2]
        phone = parts[3]

        rows.append([company, full_name, address, phone, email, units])

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
st.write("Upload a `.txt` or `.csv` directory export, and this tool will normalize it and import into Quickbase.")

uploaded_file = st.file_uploader("Upload HOA Directory File", type=["txt", "csv"])

if uploaded_file:
    # Read uploaded file
    raw_text = uploaded_file.read().decode("utf-8")

    # Detect association based on content
    detected_assoc = detect_association(raw_text)
    st.info(f"Detected Association: **{detected_assoc}**")

    # Step 1: Extract rows from messy directory text
    detected_rows = extract_table_rows(raw_text)

    rows = []

    # Step 2: Parse & normalize each row
    for parts in detected_rows:
        parsed = parse_row(parts)
        if parsed:
            # Assign detected association to ALL rows
            parsed["Association"] = detected_assoc
            rows.append(parsed)

    # Step 3: Display or error out
    if rows:
        df = pd.DataFrame(rows)
        st.success(f"Parsed {len(df)} valid rows!")
        st.dataframe(df, use_container_width=True)

        # Allow CSV download
        csv_data = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇ Download Cleaned CSV",
            csv_data,
            "hoa_cleaned.csv",
            "text/csv"
        )

        # Import into Quickbase
        if st.button("📤 Import to Quickbase"):
            results_df = send_to_quickbase(df)
            st.write("### Quickbase Import Results")
            st.dataframe(results_df)

    else:
        st.error("No valid rows were detected in this file.")


  

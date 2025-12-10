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
    Extract structured rows from multiple directory formats (HAA, AAGO, others).
    Supports:
      - HAA-style rows with email anchors
      - AAGO rows with NO email, but with phone + member profile URL
      - Mixed whitespace formats
      - Hidden phone numbers that require Selenium later
    Returns rows as:
      [company, contact_name, address, phone(or ""), email(or ""), units(or ""), url(or "")]
    """

    lines = raw_text.splitlines()
    rows = []

    email_regex = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
    phone_regex = r"\(?\d{3}\)?[-.\s]*\d{3}[-.\s]*\d{4}"
    url_regex = r"https?://\S+"

    for line in lines:
        cleaned = line.strip()
        low = cleaned.lower()

        # Skip obvious junk/header lines
        if not cleaned or len(cleaned) < 6:
            continue
        if low.startswith(("cookie", "skip to", "want to find", "search for",
                           "company name", "units greater", "company\tfull",
                           "company full", "to view complete", "member login",
                           "cart", "home", "directory")):
            continue

        # ========== CASE 1: HAA FORMAT (EMAIL ANCHOR) ==========
        email_match = re.search(email_regex, cleaned)

        if email_match:
            email = email_match.group(0)

            # Split around email
            before = cleaned[:email_match.start()].strip()
            after = cleaned[email_match.end():].strip()

            units = re.sub(r"[^\d]", "", after) if after else ""
            parts = re.split(r"\s{2,}|\t", before)

            # Fallback parsing
            if len(parts) < 4:
                tokens = before.split()
                phone_idx = None
                for i, tok in enumerate(tokens):
                    if re.match(phone_regex, tok):
                        phone_idx = i
                        break
                if phone_idx:
                    company = tokens[0]
                    full_name = tokens[1]
                    address = " ".join(tokens[2:phone_idx])
                    phone = tokens[phone_idx]
                else:
                    # skip if too messy
                    continue
            else:
                company, full_name, address, phone = parts[:4]

            rows.append([company, full_name, address, phone, email, units, ""])
            continue  # move to next line

        # ========== CASE 2: AAGO FORMAT (NO EMAIL, HAS URL + PHONE HIDDEN) ==========
        url_match = re.search(url_regex, cleaned)
        phone_match = re.search(phone_regex, cleaned)  # may not exist visually

        if url_match:
            url = url_match.group(0)
            phone = phone_match.group(0) if phone_match else ""

            # Remove URL + phone from text, leaving company + contact + address
            text_wo_url = cleaned.replace(url, "").strip()
            if phone:
                text_wo_url = text_wo_url.replace(phone, "").strip()

            parts = re.split(r"\s{2,}|\t", text_wo_url)

            # Fallback split
            tokens = text_wo_url.split()
            if len(tokens) >= 3:
                company = tokens[0]
                full_name = tokens[1]
                address = " ".join(tokens[2:])
            else:
                continue

            rows.append([company, full_name, address, phone, "", "", url])
            continue

        # ========== OTHERWISE: Not a usable row ==========
        continue

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
    """
    parts is already normalized by extract_table_rows() into:
    [company, full_name, address, phone_or_empty, email_or_empty, units_or_empty, url_or_empty]
    """

    if len(parts) < 3:
        return None  # not enough info to form a row

    company      = parts[0].strip()
    contact_name = parts[1].strip()
    address      = parts[2].strip()

    # Optional fields
    phone = parts[3].strip() if len(parts) > 3 else ""
    email = parts[4].strip() if len(parts) > 4 else ""
    units_raw = parts[5].strip() if len(parts) > 5 else ""
    profile_url = parts[6].strip() if len(parts) > 6 else ""

    # Clean units safely
    units = re.sub(r"[^\d]", "", units_raw) if units_raw else ""

    # Parse address (street, city, state, zip)
    street, city, state, zipcode = parse_address(address)

    # Unified final record format for Quickbase push
    return {
        "Company": company,
        "Property": contact_name,  # <-- You can rename this depending on meaning
        "Street": street,
        "City": city,
        "State": state,
        "Zip": zipcode,
        "Phone": phone,
        "Email": email,
        "Units": units if units else "",
        "URL": profile_url,       # needed for Selenium scrapes (AAGO)
        "Association": "UNKNOWN", # overwritten later in Streamlit
        "Member Type": "Owner",
    }




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

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time

def fetch_aago_phone(url):
    """
    Uses headless Chrome to open an AAGO profile URL and extract the phone number.
    Used only when detect_association == "AAGO".
    """

    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)

    try:
        driver.get(url)
        time.sleep(1.5)  # Allow JS to load

        # Example selector — adjust after seeing final page
        phone_elem = driver.find_element(By.CSS_SELECTOR, ".profile-phone")
        phone = phone_elem.text.strip()
        return phone

    except Exception as e:
        print("AAGO scrape error:", e)
        return ""

    finally:
        driver.quit()



# ----------------------------------------------------
# STREAMLIT UI
# ----------------------------------------------------
st.title("🏢 HOA Directory → Quickbase Import Tool")
st.write("Upload a `.txt` or `.csv` directory export, and this tool will normalize it and import into Quickbase.")

uploaded_file = st.file_uploader("Upload HOA Directory File", type=["txt", "csv"])

if uploaded_file:

    # Read file bytes → text
    raw_text = uploaded_file.read().decode("utf-8")

    # Detect association AFTER upload
    detected_assoc = detect_association(raw_text)
    st.info(f"Detected Association: **{detected_assoc}**")

    # Step 1: Extract rows from messy text
    detected_rows = extract_table_rows(raw_text)

    rows = []

    # Step 2: Parse & normalize each row
    for parts in detected_rows:
        parsed = parse_row(parts)
        if parsed:
            parsed["Association"] = detected_assoc
            rows.append(parsed)

    # Step 3: For AAGO, scrape phone numbers if needed
    if detected_assoc == "AAGO":
        st.warning("AAGO directory detected — attempting phone number extraction via Selenium.")
        
        for row in rows:
            if not row["Phone"] and row.get("URL"):
                phone = fetch_aago_phone(row["URL"])
                row["Phone"] = phone or ""
    
    # Step 4: Display or error
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




  

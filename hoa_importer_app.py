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



AAGO_COUNTIES = {
    "OSCEOLA COUNTY": "OsceolaCounty",
    "ORANGE COUNTY": "OrangeCounty",
    "SEMINOLE COUNTY": "SeminoleCounty",
    "LAKE COUNTY": "LakeCounty",
    "VOLUSIA COUNTY": "VolusiaCounty"
}


def extract_table_rows(raw_text, detected_assoc):

    lines = raw_text.splitlines()
    rows = []

    if detected_assoc != "AAGO":
        # fallback to normal HAA handler
        return extract_haa_rows(raw_text)

    # Try to detect county
    county_dir = "OsceolaCounty"
    for key, val in AAGO_COUNTIES.items():
        if key in raw_text.upper():
            county_dir = val
            break

    current = []
    seen_name = None

    for line in lines:
        l = line.strip()
        if not l:
            continue

        # -------------------------------------------
        # 1. COMMUNITY NAME (Only alphabetical words)
        # -------------------------------------------
        if len(current) == 0:
            # skip duplicate double-name (AAGO does this)
            if seen_name and l == seen_name:
                continue

            current.append(l)
            seen_name = l
            continue

        # -------------------------------------------
        # 2. STREET ADDRESS (must start with number)
        # -------------------------------------------
        if len(current) == 1 and re.match(r"^\d+ ", l):
            current.append(l)
            continue

        # -------------------------------------------
        # 3. CITY / STATE / ZIP
        # -------------------------------------------
        if len(current) == 2 and re.search(r",[ ]*[A-Z]{2}[ ]*\d{5}", l):
            current.append(l)
            continue

        # -------------------------------------------
        # 3b. OPTIONAL COUNTRY LINE
        # -------------------------------------------
        if len(current) == 3 and ("United States" in l or "USA" in l):
            # do NOT finalize yet
            current.append(l)
            continue

        # -------------------------------------------
        # 4. FINALIZING LINE → "Apartment Community"
        # -------------------------------------------
        if "Apartment Community" in l and len(current) >= 3:

            name = current[0]
            slug = re.sub(r"[^A-Za-z0-9]", "", name).lower()

            url = f"https://www.aago.org/{county_dir}/{slug}"

            # Clean up any extra optional lines before storing
            filtered = current[:3]  # name, street, city/state/zip
            filtered.append(url)

            rows.append(filtered)

            # reset
            current = []
            seen_name = None
            continue

        # -------------------------------------------
        # If none of the above matched → reset
        # (Avoid broken entries)
        # -------------------------------------------
        if len(current) > 0 and len(current) < 4:
            # unexpected junk resets block
            current = []
            seen_name = None

    return rows



    # --------------------------------------
    # CASE 2: HAA / GENERIC EMAIL-BASED DIRECTORY
    # --------------------------------------
    email_regex = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"

    for line in lines:
        cleaned = line.strip()

        # Skip non-data noise
        if not cleaned or len(cleaned) < 10:
            continue
        if cleaned.lower().startswith((
            "cookie", "skip to", "want to find", "search for",
            "company name", "units greater", "company\tfull",
            "company full", "to view complete"
        )):
            continue

        email_match = re.search(email_regex, cleaned)
        if not email_match:
            continue

        email = email_match.group(0)
        before = cleaned[:email_match.start()].strip()
        after = cleaned[email_match.end():].strip()

        units = re.sub(r"[^\d]", "", after) if after else ""

        parts = re.split(r"\s{2,}|\t", before)

        if len(parts) >= 4:
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

def parse_row(parts, detected_assoc):
    """
    Normalizes extracted row arrays into a unified Quickbase-ready dictionary.

    Formats handled:
      • AAGO: [community_name, street, city_state_zip, optional_profile_url]
      • HAA:  [company, full_name, address, phone, email, units]
      • Generic: [company, full_name, address, phone?, email?, units?, url?]
    """

    # ----------------------------------------------------
    # AAGO FORMAT (community_name, street, CSZ, URL)
    # ----------------------------------------------------
    if detected_assoc == "AAGO":
        if len(parts) < 3:
            return None  # not enough data to form a record

        name = parts[0].strip()
        street_line = parts[1].strip()
        city_state_zip = parts[2].strip()
        profile_url = parts[3].strip() if len(parts) > 3 else ""

        # Combine street + CSZ into a full address for parsing
        full_address = f"{street_line} {city_state_zip}"
        street, city, state, zipcode = parse_address(full_address)

        return {
            "Company": name,
            "Property": name,      # AAGO communities don't provide contact name
            "Street": street,
            "City": city,
            "State": state,
            "Zip": zipcode,
            "Phone": "",           # Selenium populates later
            "Email": "",
            "Units": "",
            "URL": profile_url,    # required for Selenium phone scraping
            "Association": detected_assoc,
            "Member Type": "Owner",
        }

    # ----------------------------------------------------
    # GENERIC / HAA FORMAT
    # ----------------------------------------------------
    if len(parts) < 3:
        return None  # must have at least company, name, address

    # Basic fields
    company      = parts[0].strip()
    contact_name = parts[1].strip()
    address      = parts[2].strip()

    # Optional fields (these may not exist)
    phone       = parts[3].strip() if len(parts) > 3 else ""
    email       = parts[4].strip() if len(parts) > 4 else ""
    units_raw   = parts[5].strip() if len(parts) > 5 else ""
    profile_url = parts[6].strip() if len(parts) > 6 else ""

    # Normalize units (digits only)
    units = re.sub(r"[^\d]", "", units_raw) if units_raw else ""

    # Parse address
    street, city, state, zipcode = parse_address(address)

    return {
        "Company": company,
        "Property": contact_name,
        "Street": street,
        "City": city,
        "State": state,
        "Zip": zipcode,
        "Phone": phone,
        "Email": email,
        "Units": units,
        "URL": profile_url,
        "Association": detected_assoc,
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

    # Read file → text
    raw_text = uploaded_file.read().decode("utf-8")

    # Detect association AFTER upload
    detected_assoc = detect_association(raw_text)
    st.info(f"Detected Association: **{detected_assoc}**")

    # Step 1: Extract rows based on association
    detected_rows = extract_table_rows(raw_text, detected_assoc)

    rows = []

    # Step 2: Normalize rows
    for parts in detected_rows:
        parsed = parse_row(parts, detected_assoc)
        if parsed:
            rows.append(parsed)

    # Step 3: AAGO → fetch phone numbers
    if detected_assoc == "AAGO":
        st.warning("AAGO directory detected — extracting phone numbers via Selenium...")

        for row in rows:
            url = row.get("URL", "")
            if url and not row.get("Phone"):
                phone = fetch_aago_phone(url)
                row["Phone"] = phone or ""

    # Step 4: Display or error
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

        # Import to Quickbase
        if st.button("📤 Import to Quickbase"):
            results_df = send_to_quickbase(df)
            st.write("### Quickbase Import Results")
            st.dataframe(results_df)

    else:
        st.error("No valid rows were detected in this file.")

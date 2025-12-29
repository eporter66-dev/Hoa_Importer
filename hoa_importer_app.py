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

def detect_aago_county_url(raw_text: str) -> str:
    """
    Detects which AAGO county page should be used,
    based on text inside the uploaded directory file.

    Returns a full URL such as:
        https://www.aago.org/OsceolaCounty
    """

    text = raw_text.upper()

    for key, slug in AAGO_COUNTIES.items():
        if key in text:
            return f"https://www.aago.org/{slug}"

    # Default fallback: Osceola
    return "https://www.aago.org/OsceolaCounty"



def extract_table_rows(raw_text, detected_assoc):
    """
    Extract rows from AAGO text-based directories or HAA-style directories.
    Starts parsing from 'found from search' for AAGO files.
    """

    lines = raw_text.splitlines()
    rows = []

    # -------------------------------------------------------------------
    # AAGO MODE — text-only directories with NO email, NO phone on list
    # -------------------------------------------------------------------
    if detected_assoc == "AAGO":

        # Detect county for URL building
        county_dir = "OsceolaCounty"
        for key, val in AAGO_COUNTIES.items():
            if key in raw_text.upper():
                county_dir = val
                break

        current = []
        seen_name = None
        start_found = False

        def finalize_current():
            """Append valid row"""
            if len(current) >= 3:
                rows.append(current.copy())

        for line in lines:
            l = line.strip()
            if not l:
                continue

            # Skip everything until "found from search"
            if not start_found:
                if "found from search" in l.lower():
                    start_found = True
                continue

            # Skip irrelevant labels
            if l.lower() in ("osceola county communities", "search", "list", "map"):
                continue

            # Skip duplicates or filler
            if l in ("United States", "USA"):
                continue

            # Skip summary lines like "15 found from search"
            if re.match(r"^\d+\s+found\s+from\s+search$", l.lower()):
                continue

            # ------------------------------------
            # 1. Community name
            # ------------------------------------
            if len(current) == 0:
                if seen_name == l:
                    continue # skip duplicate title line
                current.append(l)
                seen_name = l
                continue

            # ------------------------------------
            # 2. Street address (starts with number)
            # ------------------------------------
            if len(current) == 1 and re.match(r"^\d+", l):
                current.append(l)
                continue

            # ------------------------------------
            # 3. City, State ZIP
            # ------------------------------------
            if len(current) == 2 and re.search(r",[ ]*[A-Z]{2}[ ]*\d{5}", l):
                current.append(l)
                continue

            # ------------------------------------
            # 4. Apartment Community marker → finalize
            # ------------------------------------
            if "apartment community" in l.lower():
                if len(current) >= 3:
                    name = current[0]
                    slug = re.sub(r"[^A-Za-z0-9]", "", name).lower()
                    url = f"https://www.aago.org/{county_dir}/{slug}"
                    current.append(url)
                    finalize_current()
                current = []
                seen_name = None
                continue

        # Catch any last entry
        finalize_current()

        return rows

    # -------------------------------------------------------------------
    # HAA / GENERIC email-based parser
    # -------------------------------------------------------------------
    email_regex = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"

    for line in lines:
        cleaned = line.strip()
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
            rows.append([parts[0], parts[1], parts[2], parts[3], email, units])

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
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def aago_cookie_login(driver) -> bool:
    """
    Authenticates Selenium by injecting pre-authenticated AAGO cookies.
    Adds debug output so you can see where you actually land (login vs logged in).
    """
    try:
        # 1) Load the base domain first (required before add_cookie)
        driver.get("https://www.aago.org/")
        time.sleep(1)

        st.write("AAGO step: loaded base page")
        st.write("AAGO URL (base):", driver.current_url)

        # 2) Load cookies from Streamlit secrets
        if "AAGO_COOKIES" not in st.secrets:
            st.error("Missing AAGO_COOKIES in Streamlit secrets.")
            return False

        cookies = json.loads(st.secrets["AAGO_COOKIES"])

        # 3) Inject cookies (do NOT force domain unless the cookie includes it)
        for cookie in cookies:
            cookie_dict = {
                "name": cookie["name"],
                "value": cookie["value"],
                "path": cookie.get("path", "/"),
            }

            # Only set domain if provided (host-only cookies break if you force a domain)
            if cookie.get("domain"):
                cookie_dict["domain"] = cookie["domain"]

            # Pass through common optional fields if present
            for k in ("secure", "httpOnly", "expiry", "sameSite"):
                if cookie.get(k) is not None:
                    cookie_dict[k] = cookie[k]

            driver.add_cookie(cookie_dict)

        st.write("AAGO step: cookies injected")
        st.write("AAGO cookies now in browser:", [c["name"] for c in driver.get_cookies()][:15])

        # 4) Refresh to apply authenticated session
        driver.refresh()
        time.sleep(2)

        st.write("AAGO URL (after refresh):", driver.current_url)

        # 5) Fail fast if we got redirected to login
        if "login" in driver.current_url.lower():
            st.error(f"AAGO appears unauthenticated (redirected to login): {driver.current_url}")
            return False

        return True

    except Exception as e:
        st.error(f"AAGO cookie login failed: {e}")
        return False







def fetch_aago_urls(driver, county_url):
    driver.get(county_url)
    time.sleep(2)

    st.write("AAGO URL (county page):", driver.current_url)

    # If we got bounced to login here, cookies didn't apply to this host/path
    if "login" in driver.current_url.lower():
        raise RuntimeError(f"Redirected to login when opening county page: {driver.current_url}")

    results = {}

    cards = driver.find_elements(By.CSS_SELECTOR, ".directory-item")

    for card in cards:
        try:
            name = card.find_element(By.CSS_SELECTOR, "h3").text.strip()
            href = card.find_element(By.CSS_SELECTOR, "a").get_attribute("href")

            if href.startswith("/"):
                href = "https://www.aago.org" + href

            results[name] = href
        except:
            continue

    return results







def fetch_aago_profile(driver, url):
    """
    Scrapes an authenticated AAGO profile page to extract:
        • Phone number
        • Email placeholder ("CONTACT_FORM") if message button exists

    Assumes:
        ✔ driver is already logged in
        ✔ driver session is authenticated
    """

    PHONE_REGEX = re.compile(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")

    result = {"Phone": "", "Email": ""}

    try:
        driver.get(url)
        time.sleep(2)

        st.write("AAGO URL (profile):", driver.current_url)

        if "login" in driver.current_url.lower():
            raise RuntimeError(f"Redirected to login when opening profile: {driver.current_url}")

        # -------------------------
        # PHONE SCRAPING (scoped)
        # -------------------------
        info_blocks = driver.find_elements(
            By.CSS_SELECTOR,
            ".info-section p, .contact-info p"
        )

        for el in info_blocks:
            match = PHONE_REGEX.search(el.text)
            if match:
                result["Phone"] = match.group()
                break

        # -------------------------
        # CONTACT FORM DETECTION
        # -------------------------
        for a in driver.find_elements(By.TAG_NAME, "a"):
            if "message" in a.text.lower():
                result["Email"] = "CONTACT_FORM"
                break

        return result

    except Exception as e:
        st.error(f"AAGO profile scrape error: {e}")
        return result


# ----------------------------------------------------
# STREAMLIT UI
# ----------------------------------------------------
st.title("🏢 HOA Directory → Quickbase Import Tool")
st.write(
    "Upload a `.txt` or `.csv` directory export, and this tool will normalize it "
    "and prepare it for Quickbase import."
)

uploaded_file = st.file_uploader("Upload HOA Directory File", type=["txt", "csv"])

if uploaded_file:

    # -----------------------------
    # Read uploaded file
    # -----------------------------
    raw_text = uploaded_file.read().decode("utf-8")

    # Detect association
    detected_assoc = detect_association(raw_text)
    st.info(f"Detected Association: **{detected_assoc}**")

    # -----------------------------
    # Extract + normalize rows
    # -----------------------------
    detected_rows = extract_table_rows(raw_text, detected_assoc)
    rows = []

    for parts in detected_rows:
        parsed = parse_row(parts, detected_assoc)
        if parsed:
            rows.append(parsed)

    # -----------------------------
    # AAGO SPECIAL WORKFLOW
    # -----------------------------
    if detected_assoc == "AAGO" and rows:
        st.warning("AAGO directory detected — fetching profile details using Selenium...")

        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")

        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )


        driver = webdriver.Chrome(options=chrome_options)

        try:
            # 1️⃣ LOGIN ONCE
            if not aago_cookie_login(driver):
                st.error("Unable to log into AAGO. Scraping aborted.")
            else:
                # 2️⃣ BUILD URL MAP
                county_url = detect_aago_county_url(raw_text)
                try:
                    url_map = fetch_aago_urls(driver, county_url)
                except Exception as e:
                    st.error(f"Failed while loading county URL map: {e}")
                    url_map = {}


                for row in rows:
                    name = row["Company"]
                    if name in url_map:
                        row["URL"] = url_map[name]

                # 3️⃣ SCRAPE PROFILES
                progress = st.progress(0.0)

                for i, row in enumerate(rows):
                    url = row.get("URL")
                    if url:
                        profile = fetch_aago_profile(driver, url)
                        row["Phone"] = profile.get("Phone", "")
                        row["Email"] = profile.get("Email", "")

                    progress.progress((i + 1) / len(rows))

                st.success("AAGO profiles successfully scanned!")

        finally:
            driver.quit()

    # -----------------------------
    # Display results or error
    # -----------------------------
    if rows:
        df = pd.DataFrame(rows)

        st.success(f"Parsed {len(df)} valid rows!")
        st.dataframe(df, use_container_width=True)

        # -----------------------------
        # CSV Download
        # -----------------------------
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇ Download Cleaned CSV",
            data=csv_bytes,
            file_name="hoa_cleaned.csv",
            mime="text/csv",
        )

        # -----------------------------
        # Quickbase Import
        # -----------------------------
        if st.button("📤 Import to Quickbase"):
            results_df = send_to_quickbase(df)
            st.write("### Quickbase Import Results")
            st.dataframe(results_df, use_container_width=True)

    else:
        st.error("No valid rows were detected in this file.")

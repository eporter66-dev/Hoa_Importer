import streamlit as st
import pandas as pd
import json
import csv
import re
import requests
from io import StringIO
import base64
import os




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
            "Member Type": "Member"
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
        "Member Type": "Member",
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
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
import tempfile
import shutil



def find_password_in_shadow_dom(driver):
    script = """
    function firstPassword(root) {
      if (!root) return null;

      // Search within this root
      try {
        const el = root.querySelector && root.querySelector("input[type='password']");
        if (el) return el;
      } catch(e) {}

      // Traverse element children
      const kids = root.children || root.childNodes || [];
      for (const k of kids) {
        const found = firstPassword(k);
        if (found) return found;
      }

      // Traverse shadow root
      if (root.shadowRoot) {
        const found = firstPassword(root.shadowRoot);
        if (found) return found;
      }

      return null;
    }
    return firstPassword(document.documentElement);
    """
    return driver.execute_script(script)




def st_screenshot(driver, label="screenshot"):
    png = driver.get_screenshot_as_png()
    st.markdown(f"**{label}**")
    st.image(png, use_container_width=True)

def st_bot_gate_signals(driver):
    html = driver.page_source.lower()
    tokens = ["recaptcha", "g-recaptcha", "hcaptcha", "cf-challenge", "cloudflare", "verify you are", "unusual traffic"]
    found = [t for t in tokens if t in html]
    st.write("Bot-gate signals found:", found)
    return found




def _find_email_input_anywhere(driver, timeout=20):
    """
    Returns (email_input, frame_index_or_None).
    If frame_index is not None, driver is left switched into that iframe.
    """
    wait = WebDriverWait(driver, timeout)

    selectors = [
        "input[type='email']",
        "input[autocomplete='username']",
        "input[name*='email' i]",
        "input[id*='email' i]",
        "input[placeholder*='email' i]",
        "input[aria-label*='email' i]",
        "input[type='text']",
    ]

    # 1) main document
    driver.switch_to.default_content()
    for sel in selectors:
        try:
            el = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, sel)))
            if el.is_displayed() and el.is_enabled():
                return el, None
        except Exception:
            pass

    # 2) iframes
    driver.switch_to.default_content()
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    for i, iframe in enumerate(iframes):
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame(iframe)

            for sel in selectors:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                els = [e for e in els if e.is_displayed() and e.is_enabled()]
                if els:
                    return els[0], i
        except Exception:
            continue

    driver.switch_to.default_content()
    raise TimeoutException("No visible email input found in main page or any iframe.")


def _fill_email_reliably(driver, el, email: str) -> str:
    """Fill email and ensure it 'sticks' (click + clear + send_keys + JS value + input/change events)."""

    # scroll + click
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    time.sleep(0.2)
    try:
        el.click()
    except Exception:
        driver.execute_script("arguments[0].click();", el)

    # clear
    try:
        el.clear()
    except Exception:
        pass
    try:
        el.send_keys(Keys.CONTROL, "a")
        el.send_keys(Keys.DELETE)
    except Exception:
        pass

    # type first (some sites only “activate” validation on real keystrokes)
    try:
        el.send_keys(email)
    except Exception:
        pass
    time.sleep(0.2)

    # then force value + events (safe InputEvent fallback)
    driver.execute_script(
        """
        const el = arguments[0];
        const val = arguments[1];
        el.focus();
        el.value = val;

        try {
          el.dispatchEvent(new InputEvent('input', { bubbles: true }));
        } catch (e) {
          el.dispatchEvent(new Event('input', { bubbles: true }));
        }
        el.dispatchEvent(new Event('change', { bubbles: true }));
        """,
        el, email
    )

    time.sleep(0.2)
    return (el.get_attribute("value") or "").strip()




def aago_password_login(driver) -> bool:
    """
    Logs into AAGO (2-step):
      Step 1: Email + Continue
      Step 2: Password appears + submit
    """
    # -----------------------------
    # Small helpers
    # -----------------------------
    def _click_by_text_any(texts, timeout=6) -> bool:
        """Click a button/link/input whose visible text/value contains any text (case-insensitive)."""
        end = time.time() + timeout
        up = "abcdefghijklmnopqrstuvwxyz"
        lo = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        while time.time() < end:
            for t in texts:
                try:
                    xpath = (
                        "//*[self::button or self::a or (self::input and (@type='submit' or @type='button'))]"
                        f"[contains(translate(normalize-space(string(.)),'{up}','{lo}'),'{t.upper()}') "
                        f"or contains(translate(@value,'{up}','{lo}'),'{t.upper()}')]"
                    )
                    els = driver.find_elements(By.XPATH, xpath)
                    els = [e for e in els if e.is_displayed() and e.is_enabled()]
                    if els:
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", els[0])
                        try:
                            els[0].click()
                        except Exception:
                            driver.execute_script("arguments[0].click();", els[0])
                        return True
                except Exception:
                    continue
            time.sleep(0.2)
        return False

    def _visible_inputs_in_context(driver):
        ins = driver.find_elements(By.CSS_SELECTOR, "input")
        return [i for i in ins if i.is_displayed() and i.is_enabled()]

    def _find_password_like_input_anywhere(timeout=30):
        selectors = [
            "input[type='password']",
            "input[autocomplete='current-password']",
            "input[name*='pass' i]",
            "input[id*='pass' i]",
            "input[aria-label*='pass' i]",
            "input[placeholder*='pass' i]",
            "input[autocomplete='password']",
        ]

        # 1) Main document
        driver.switch_to.default_content()
        w = WebDriverWait(driver, timeout)
        for sel in selectors:
            try:
                el = w.until(EC.visibility_of_element_located((By.CSS_SELECTOR, sel)))
                return el, None
            except Exception:
                continue

        # 2) Shadow DOM (optional helper)
        try:
            shadow_pass = find_password_in_shadow_dom(driver)
            if shadow_pass:
                return shadow_pass, None
        except Exception:
            pass

        # 3) Iframes
        driver.switch_to.default_content()
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        for i, iframe in enumerate(iframes):
            try:
                driver.switch_to.default_content()
                driver.switch_to.frame(iframe)

                for sel in selectors:
                    els = driver.find_elements(By.CSS_SELECTOR, sel)
                    els = [e for e in els if e.is_displayed() and e.is_enabled()]
                    if els:
                        return els[0], i

                inputs = _visible_inputs_in_context(driver)
                if inputs and "password" in driver.page_source.lower():
                    return inputs[-1], i

            except Exception:
                continue

        driver.switch_to.default_content()
        raise TimeoutException("No password-like input found in main page, shadow DOM, or any iframe.")

    def _fill_input(el, value: str):
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        except Exception:
            pass
        try:
            el.click()
        except Exception:
            try:
                driver.execute_script("arguments[0].click();", el)
            except Exception:
                pass
        try:
            el.send_keys(Keys.CONTROL, "a")
        except Exception:
            pass
        el.send_keys(value)

    # -----------------------------
    # Main flow
    # -----------------------------
    try:
        if "AAGO_EMAIL" not in st.secrets or "AAGO_PASSWORD" not in st.secrets:
            st.error("Missing AAGO_EMAIL or AAGO_PASSWORD in Streamlit secrets.")
            return False

        email = st.secrets["AAGO_EMAIL"]
        password = st.secrets["AAGO_PASSWORD"]

        wait = WebDriverWait(driver, 30)

        # Open login
        driver.get("https://www.aago.org/login")
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(1.0)

        st.write("AAGO URL (login):", driver.current_url)
        st.write("AAGO Title (login):", driver.title)

        try:
            st_screenshot(driver, "AAGO login page (Selenium view)")
        except Exception:
            pass

        # Bot/captcha signal detection
        try:
            _ = st_bot_gate_signals(driver)
            captcha_widgets = driver.find_elements(
                By.CSS_SELECTOR,
                ".g-recaptcha, [data-sitekey], iframe[src*='recaptcha'], iframe[src*='hcaptcha']"
            )
            captcha_visible = [c for c in captcha_widgets if c.is_displayed()]
            if captcha_visible:
                st.error("Captcha widget detected (visible). Headless Selenium cannot proceed.")
                try:
                    st_screenshot(driver, "Captcha widget detected (visible)")
                except Exception:
                    pass
                return False
        except Exception:
            pass

        # Dismiss cookie/modal banner if present
        try:
            _click_by_text_any(["ACKNOWLEDGE", "ACCEPT", "I AGREE", "GOT IT"], timeout=3)
        except Exception:
            pass

        # --------------------------
        # STEP 1: Email + Continue
        # --------------------------

        driver.switch_to.default_content()

        # Try to dismiss banners that steal focus
        try:
            driver.execute_script("window.scrollTo(0, 0);")
            _click_by_text_any(["IGNORE", "CLOSE", "ACCEPT", "ACKNOWLEDGE", "I AGREE", "GOT IT"], timeout=2)
        except Exception:
            pass

        # Find + fill email (may be in iframe)
        email_input, email_frame = _find_email_input_anywhere(driver, timeout=20)
        st.write("Email field found in:", "main page" if email_frame is None else f"iframe #{email_frame}")

        filled_val = _fill_email_reliably(driver, email_input, email)

        try:
            st_screenshot(driver, "After filling email (verify visible value)")
        except Exception:
            pass

        st.write("Email value read back:", repr(filled_val))

        # HARD STOP if it didn't stick
        if not filled_val or "@" not in filled_val:
            raise RuntimeError("Email did not populate (value still blank) — cannot proceed.")

        # --------------------------
        # CLICK THE REAL CONTINUE BUTTON (type='button', not a form submit)
        # --------------------------
        start_url = driver.current_url

        # Ensure we're in the same context as the email input when clicking Continue
        driver.switch_to.default_content()
        if email_frame is not None:
            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            if 0 <= email_frame < len(iframes):
                driver.switch_to.frame(iframes[email_frame])

        # Click the specific AAGO Continue button
        try:
            continue_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.c-login-form__continue-button"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", continue_btn)
            driver.execute_script("arguments[0].click();", continue_btn)
            st.write("Continue: clicked .c-login-form__continue-button")
        except Exception as e:
            st.write("Continue: failed clicking .c-login-form__continue-button:", str(e))
            # fallback: try by visible text in the current context
            try:
                clicked = _click_by_text_any(["CONTINUE", "NEXT"], timeout=3)
                st.write("Continue: fallback clicked by text =", clicked)
            except Exception:
                pass

        # Always return to default content for subsequent checks/screenshots
        driver.switch_to.default_content()

        time.sleep(1)
        st.write("URL after Continue:", driver.current_url)
        st.write("Title after Continue:", driver.title)
        try:
            st_screenshot(driver, "After Continue (immediate)")
        except Exception:
            pass

        # --------------------------
        # Wait for next step (AJAX may keep you on /login)
        # --------------------------
        def _next_step_ready(d):
            html = d.page_source.lower()

            # password inputs appear
            if d.find_elements(By.CSS_SELECTOR, "input[type='password'], input[autocomplete='current-password'], input[autocomplete='password']"):
                return True

            # or page contains password-related UI text
            if "password" in html:
                return True

            # or AAGO shows other next-step states
            if "set password" in html:
                return True
            if "try again" in html:
                return True
            if "create account" in html:
                return True

            # or you navigated away
            if d.current_url != start_url:
                return True

            return False

        try:
           WebDriverWait(driver, 20).until(_next_step_ready)
        except Exception:
            pass

        try:
            st_screenshot(driver, "After Continue (before password search)")
        except Exception:
            pass

        

        # --------------------------
        # STEP 2: Password + submit
        # --------------------------
        pass_input, frame_index = _find_password_like_input_anywhere(timeout=30)
        st.write("Password field found in:", "main page" if frame_index is None else f"iframe #{frame_index}")

        try:
            WebDriverWait(driver, 10).until(lambda d: pass_input.is_displayed() and pass_input.is_enabled())
        except Exception:
            time.sleep(1)

        _fill_input(pass_input, password)

        submitted = False
        try:
            form = pass_input.find_element(By.XPATH, "ancestor::form[1]")
            btns = form.find_elements(By.CSS_SELECTOR, "button[type='submit'], input[type='submit']")
            btns = [b for b in btns if b.is_displayed() and b.is_enabled()]
            if btns:
                try:
                    btns[0].click()
                except Exception:
                    driver.execute_script("arguments[0].click();", btns[0])
                submitted = True
        except Exception:
            pass

        if not submitted:
            submitted = _click_by_text_any(["SIGN IN", "LOG IN", "SUBMIT"], timeout=4)

        if not submitted:
            try:
                pass_input.send_keys(Keys.ENTER)
            except Exception:
                pass

        time.sleep(2.0)
        driver.switch_to.default_content()

        st.write("AAGO URL (after submit):", driver.current_url)
        st.write("AAGO Title (after submit):", driver.title)

        try:
            st_screenshot(driver, "After password submit (Selenium view)")
        except Exception:
            pass

        if "login" in driver.current_url.lower():
            st.error("Still on login page after password submit.")
            try:
                errs = driver.find_elements(By.CSS_SELECTOR, ".error, .alert, .message, .validation-summary-errors")
                msgs = [e.text.strip() for e in errs if e.is_displayed() and e.text.strip()]
                if msgs:
                    st.write("Login page messages:", msgs[:5])
            except Exception:
                pass
            return False

        return True

    except TimeoutException as e:
        try:
            driver.switch_to.default_content()
        except Exception:
            pass
        st.error(f"AAGO password login failed (Timeout): {e}")
        st.write("URL at failure:", driver.current_url)
        st.write("Title at failure:", driver.title)
        try:
            st_screenshot(driver, "Timeout failure (Selenium view)")
        except Exception:
            pass
        st.code(driver.page_source[:2500])
        return False

    except Exception as e:
        try:
            driver.switch_to.default_content()
        except Exception:
            pass
        st.error(f"AAGO password login failed: {e}")
        st.write("URL at failure:", driver.current_url)
        st.write("Title at failure:", driver.title)
        try:
            st_screenshot(driver, "Exception failure (Selenium view)")
        except Exception:
            pass
        st.code(driver.page_source[:2500])
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

        # ---- create unique writable dirs per run (prevents /tmp collisions) ----
        profile_dir = tempfile.mkdtemp(prefix="chrome-profile-")
        cache_dir   = tempfile.mkdtemp(prefix="chrome-cache-")

        chrome_options = Options()
        
        chrome_options.add_argument("--disable-application-cache")
        chrome_options.add_argument("--disk-cache-size=0")
        chrome_options.add_argument("--media-cache-size=0")


        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")

        # Helps stability in container envs
        chrome_options.add_argument("--remote-debugging-port=0")  # let Chrome pick a free port
        chrome_options.add_argument("--disable-software-rasterizer")

        # Use unique profile/cache dirs
        chrome_options.add_argument(f"--user-data-dir={profile_dir}")
        chrome_options.add_argument(f"--disk-cache-dir={cache_dir}")

        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )


        

        driver = None
        try:
            driver = webdriver.Chrome(service=Service(), options=chrome_options)

            # 1) LOGIN ONCE
            st.write("Starting AAGO password login…")
            if not aago_password_login(driver):
                st.error("Unable to log into AAGO. Scraping aborted.")
            else:
                # 2) BUILD URL MAP
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

                # 3) SCRAPE PROFILES
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
            # quit chrome cleanly
            try:
                if driver:
                    driver.quit()
            except Exception:
                pass

            # remove temp dirs
            try:
                shutil.rmtree(profile_dir, ignore_errors=True)
                shutil.rmtree(cache_dir, ignore_errors=True)
            except Exception:
                pass


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

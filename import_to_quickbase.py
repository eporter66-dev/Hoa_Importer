import requests
import csv
import json
import re

# ----------------------------------------------------
# QUICKBASE CONFIG
# ----------------------------------------------------
QB_REALM = "rotoloconsultants.quickbase.com"
QB_TABLE_ID = "bu9vgbhdg"
QB_USER_TOKEN = "b9qytm_ppjb_0_d9gvuvvsa8iecbxbyc9mciwq6m5"

# ----------------------------------------------------
# ADDRESS PARSER (robust)
# ----------------------------------------------------
def parse_address(full_address):
    """
    Normalizes and splits full addresses into street, city, state, zip.
    Accepts formats like:
    - '25399 Ramrock Dr Porter TX 77365'
    - '8750 N Central Expy #1010 Dallas, TX 75231'
    - '1909 Woodall Rodgers Fwy #400 Dallas, TX 75201-2274 UNITED STATES'
    """

    if not full_address:
        return "", "", "", ""

    addr = full_address.replace(",", "").strip()
    parts = addr.split()

    # Remove trailing country (UNITED STATES, USA)
    if len(parts) >= 2 and parts[-2].upper() in ["UNITED", "USA"] and parts[-1].upper() in ["STATES", "AMERICA"]:
        parts = parts[:-2]

    if len(parts) < 4:
        print(f"⚠️ Cannot parse address: '{full_address}'")
        return full_address, "", "", ""

    zipcode = parts[-1]
    state = parts[-2]
    city = parts[-3]
    street = " ".join(parts[:-3])

    print(f"   ✔ Parsed Addr → Street='{street}', City='{city}', State='{state}', Zip='{zipcode}'")
    return street, city, state, zipcode


# ----------------------------------------------------
# PRIMARY ROW PARSER — STANDARDIZES ALL ROWS TO FORMAT B
# ----------------------------------------------------
def parse_row(parts, idx):
    print(f"   🔎 Row {idx}: {len(parts)} columns → {parts}")

    # Skip obvious noise
    row_text = " ".join(parts).lower()
    if len(parts) < 6 or "```" in row_text or "..." in row_text or "there are" in row_text:
        print(f"   ⚠️ Skipping junk row {idx}")
        return None

    # ----------------------------------------------------
    # FORMAT B — Already complete (11 columns)
    # ----------------------------------------------------
    if len(parts) == 11:
        return {
            "company": parts[0].strip(),
            "property_name": parts[1].strip(),
            "street": parts[2].strip(),
            "city": parts[3].strip(),
            "state": parts[4].strip(),
            "zip": parts[5].strip(),
            "phone": parts[6].strip(),
            "email": parts[7].strip(),
            "units": parts[8].strip(),
            "association": parts[9].strip() or "HAA",
            "member_type": parts[10].strip() or "Owner",
        }

    # ----------------------------------------------------
    # FORMAT A — (6 columns → convert to Format B)
    # ----------------------------------------------------
    if len(parts) == 6:
        company, prop, full_addr, phone, email, units = map(str.strip, parts)
        street, city, state, zipcode = parse_address(full_addr)

        return {
            "company": company,
            "property_name": prop,
            "street": street,
            "city": city,
            "state": state,
            "zip": zipcode,
            "phone": phone,
            "email": email,
            "units": units,
            "association": "HAA",
            "member_type": "Owner",
        }

    # ----------------------------------------------------
    # FORMAT C — (7 columns → merge addr → convert to Format B)
    # ----------------------------------------------------
    if len(parts) == 7:
        company, prop, a1, a2, phone, email, units = map(str.strip, parts)
        merged = f"{a1} {a2}"
        street, city, state, zipcode = parse_address(merged)

        return {
            "company": company,
            "property_name": prop,
            "street": street,
            "city": city,
            "state": state,
            "zip": zipcode,
            "phone": phone,
            "email": email,
            "units": units,
            "association": "HAA",
            "member_type": "Owner",
        }

    print(f"   ⚠️ Unknown format → Skipping row {idx}")
    return None


# ----------------------------------------------------
# IMPORT FUNCTION — Quickbase INSERT
# ----------------------------------------------------
def import_csv_to_quickbase(csv_path):
    url = "https://api.quickbase.com/v1/records"

    headers = {
        "QB-Realm-Hostname": QB_REALM,
        "Authorization": f"QB-USER-TOKEN {QB_USER_TOKEN}",
        "Content-Type": "application/json",
    }

    print(f"\n📁 Loading CSV: {csv_path}\n")
    rows_processed = 0

    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        header = next(reader)
        print(f"📌 CSV Header: {header}\n")

        for idx, parts in enumerate(reader, start=1):
            print("\n====================================================")
            print(f"➡️ PROCESSING ROW {idx}")

            row = parse_row(parts, idx)
            if not row:
                continue

            # Normalize units into pure integer
            units_clean = int(re.sub(r"[^\d]", "", row["units"])) if row["units"] else 0

            # Build Quickbase record (correct JSON format)
            record = {
                "6":  {"value": row["association"]},
                "7":  {"value": row["property_name"]},
                "8":  {"value": row["member_type"]},

                "9":  {"value": f"{row['street']} {row['city']} {row['state']} {row['zip']}"},
                "10": {"value": row["city"]},
                "11": {"value": row["state"]},
                "12": {"value": row["zip"]},

                "14": {"value": row["phone"]},
                "15": {"value": row["email"]},
                "18": {"value": row["company"]},

                "22": {"value": units_clean},
            }

            payload = {"to": QB_TABLE_ID, "data": [record]}

            print("\n📨 PAYLOAD TO QUICKBASE:")
            print(json.dumps(payload, indent=4))

            resp = requests.post(url, headers=headers, json=payload)
            print(f"📨 HTTP {resp.status_code}")
            print(f"📨 RESPONSE: {resp.text}")

            if resp.status_code != 200:
                print(f"❌ Import stopped on row {idx}")
                return

            rows_processed += 1

    print(f"\n🎉 IMPORT COMPLETE — {rows_processed} rows successfully sent to Quickbase.\n")


# ----------------------------------------------------
# RUN
# ----------------------------------------------------
if __name__ == "__main__":
    import_csv_to_quickbase("/home/rci/Desktop/associationLists/final.csv")

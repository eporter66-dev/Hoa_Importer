import requests
import json

QB_REALM = "rotoloconsultants.quickbase.com"   # <-- Update if needed
QB_TABLE_ID = "bu9vgbhdg"                      # <-- Your table ID
QB_USER_TOKEN = "b9qytm_ppjb_0_d9gvuvvsa8iecbxbyc9mciwq6m5"              # <-- same token you use in Node backend

url = f"https://api.quickbase.com/v1/fields?tableId={QB_TABLE_ID}"

headers = {
    "QB-Realm-Hostname": QB_REALM,
    "Authorization": f"QB-USER-TOKEN {QB_USER_TOKEN}",
    "Content-Type": "application/json"
}

print("📡 Fetching REAL Quickbase schema via GET /fields ...\n")

response = requests.get(url, headers=headers)

print("HTTP", response.status_code)
print()

data = response.json()

print("========== FIELD SCHEMA ==========\n")

for f in data:
    print(
        f"fid={f['id']:<4} "
        f"type={f['fieldType']:<15} "
        f"label={f['label']:<30} "
        f"required={f.get('required', False)} "
        f"noUpdate={f.get('noUpdate', False)} "
        f"mode={f.get('mode','')} "
    )

import os
import subprocess

INPUT_FILE = "/home/rci/Desktop/associationLists/1.txt"
OUTPUT_FILE = "/home/rci/Desktop/associationLists/final.csv"

CHUNK_SIZE = 80  # lines per chunk

def run_ollama(prompt):
    result = subprocess.run(
        ["ollama", "run", "llama3.1"],
        input=prompt.encode(),
        stdout=subprocess.PIPE
    )
    return result.stdout.decode()

def chunk_lines(lines, size):
    for i in range(0, len(lines), size):
        yield lines[i:i+size]

with open(INPUT_FILE, "r") as f:
    lines = f.readlines()

all_rows = []
header_written = False

for idx, chunk in enumerate(chunk_lines(lines, CHUNK_SIZE)):
    text = "".join(chunk)

    prompt = f"""
You are a data extraction AI.

The following text is part of a longer list of apartment properties.
Extract ONLY rows in CSV format with columns:

Company, Property, Address, Phone, Email, Units

Rules:
- DO NOT hallucinate values.
- Only extract rows that match the pattern.
- NO explanations. Output ONLY raw CSV rows.
- DO NOT repeat the header after chunk 1.

TEXT:
{text}
"""

    print(f"Processing chunk {idx+1}...")
    csv_out = run_ollama(prompt)

    # split into lines
    for line in csv_out.split("\n"):
        if not line.strip():
            continue
        if not header_written:
            all_rows.append(line)
            header_written = True
        else:
            if "Company" not in line:
                all_rows.append(line)

# Write final CSV
with open(OUTPUT_FILE, "w") as f:
    f.write("\n".join(all_rows))

print(f"Done! CSV written to: {OUTPUT_FILE}")

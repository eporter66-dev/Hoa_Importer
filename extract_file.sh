#!/bin/bash

INPUT_FILE="/home/rci/Desktop/associationLists/1.txt"
OUTPUT_FILE="/home/rci/Desktop/associationLists/1.csv"

echo "Processing $INPUT_FILE ..."

# Encode safely
B64=$(base64 -w 0 "$INPUT_FILE")

# Build JSON payload
read -r -d '' JSON <<EOF
{
  "prompt": "You are a data extraction AI.\nThe following is BASE64-ENCODED text copied from a webpage. Decode it and extract the tabular data into a CSV with the following columns:\nCompany, Property, Address, Phone, Email, Units.\n\nRules:\n- Detect the start of the table at the header line: 'Company Property Address Phone Email Units'.\n- Each row appears to be separated by line breaks.\n- Preserve full multi-word values.\n- Do not invent values.\n- Output ONLY a CSV (no explanations).\n\nBASE64_CONTENT:\n$B64"
}
EOF

# Run using Ollama STDIN API
echo "$JSON" | ollama run llama3.1 > "$OUTPUT_FILE"

echo "Done! CSV saved to: $OUTPUT_FILE"

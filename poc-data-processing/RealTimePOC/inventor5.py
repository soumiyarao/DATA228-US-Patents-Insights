from pyspark.sql import SparkSession
import requests
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("LensAPI Patent Data") \
    .getOrCreate()

# Define the API URL and token
API_URL = "https://api.lens.org/patent/search"
TOKEN = "A5NTTH4NTeEQHU3h63k3J2Fo3POHxbei6bSOGyU9fdGXl8RAVyZi"

# Define the POST body
post_body = {
    "query": {
        "bool": {
            "must": [
                {
                    "query_string": {
                        "query": "class_cpc.symbol:(G06N OR G06F19\\/00 OR G06F17\\/27 OR G06F17\\/28 OR G10L15\\/22 OR G06T OR G06K9\\/00)"
                    }
                }
            ],
            "filter": [
                {"term": {"has_claim": True}},
                {"term": {"has_inventor": True}},
                {"term": {"has_applicant": True}}
            ]
        }
    },
    "size": 100,
    "include": [
        "doc_number",
        "date_published",
        "claims",
        "biblio.parties.inventors.extracted_name",
        "biblio.parties.applicants.extracted_name"
    ],
    "sort": [
        {
            "date_published": "desc"
        }
    ]
}

# Make a POST request to the Lens.org API
headers = {"Content-Type": "application/json"}
response = requests.post(API_URL, headers=headers, json=post_body, params={"token": TOKEN})

# Check the response status
if response.status_code == 200:
    # Parse the response JSON
    data = response.json().get("data", [])
    print("Data retrieved successfully.")

    transformed_data = []

    # Process each record in the "data" array
    for record in data:

        # Safely extract the nested claims field
        claims = record.get("claims", [])
        nested_claims = [claim.get("claims", []) for claim in claims]
        total_num_claims = sum(len(claim_group) for claim_group in nested_claims)
       # Map fields to the desired structure
        transformed_record = {
            "patent_id": record.get("doc_number", None),  # Map doc_number to patent_id
            "num_claims": total_num_claims,  # Count the length of nested claims
            "patent_date": record.get("date_published", None),  # Map date_published to patent_date
            "branch": "Artificial Intelligence",  # Set branch to a constant value
            "inventors": [
                {
                    "inventor_name": inventor.get("extracted_name", {}).get("value", None),  # Extract inventor name
                    "gender": "U"  # Gender is unavailable, default to "U" (Unknown)
                }
                for inventor in record.get("biblio", {})
                               .get("parties", {})
                               .get("inventors", [])
                if inventor.get("extracted_name", {}).get("value", None)  # Ensure name is valid
            ],
            "applicants": [
                {
                    "organization": applicant.get("extracted_name", {}).get("value", None)  # Extract organization name
                }
                for applicant in record.get("biblio", {})
                               .get("parties", {})
                               .get("applicants", [])
                if applicant.get("extracted_name", {}).get("value", None)  # Ensure organization name is valid
            ]
        }
        # Append the transformed record to the new JSON list
        transformed_data.append(transformed_record)

    # Write transformed_data to a JSON file
    output_file = "transformed_patents.json"
    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(transformed_data, file, indent=4, ensure_ascii=False)
else:
    print(f"Error: {response.status_code}, {response.text}")

# Stop Spark session
spark.stop()

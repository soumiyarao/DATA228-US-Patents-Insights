import requests
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the API URL and token
API_URL = "https://api.lens.org/patent/search"
TOKEN = "A5NTTH4NTeEQHU3h63k3J2Fo3POHxbei6bSOGyU9fdGXl8RAVyZi"

# Branches for the analytic purpose
branches_data = {
    "Artificial Intelligence": r"class_cpc.symbol:(G06N OR G06F19\/00 OR G06F17\/27 OR G06F17\/28 OR G10L15\/22 OR G06T OR G06K9\/00)",
    "Data Science and Analytics": r"class_cpc.symbol:(G06F18\/20 OR G06F18\/22 OR G06F16\/10 OR G06F16\/20 OR G06F16\/30 OR G06F16\/40)",
    "Networking and Distributed Systems": r"class_cpc.symbol:(H04L29\/08 OR H04L12\/16 OR H04L12\/22 OR H04L12\/26)",
    "Software Development and Security": r"class_cpc.symbol:(G06F21\/00 OR G06F21\/10 OR G06F21\/12 OR G06F21\/30 OR G06F21\/40)",
    "Advanced Computing Technologies": r"class_cpc.symbol:(G06F9\/50 OR G06F9\/54 OR G06F9\/30 OR G06F15\/16)"
}
# monthly_data is considered to keep the record of patents
monthly_data = {}

# Function to fetch the data from Lens API and process it
def process_branch(branch_name, query_string):
    logging.info(f"Processing branch: {branch_name}")   
    # Define the POST body
    post_body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "query_string": {
                            "query": query_string
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
                "date_published": "desc"  # for the new records to be accessed
            }
        ]
    }

    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(API_URL, headers=headers, json=post_body, params={"token": TOKEN})

        if response.status_code == 200:
            raw_data = response.json().get("data", [])

            for record in raw_data:
                claims = record.get("claims", [])
                nested_claims = [claim.get("claims", []) for claim in claims]
                total_num_claims = sum(len(claim_group) for claim_group in nested_claims)

                transformed_record = {
                    "patent_id": record.get("doc_number"),
                    "num_claims": total_num_claims,
                    "patent_date": record.get("date_published"),
                    "branch": branch_name,
                    "inventors": [
                        {
                            "inventor_name": inventor.get("extracted_name", {}).get("value"),
                            "gender": "U"
                        }
                        for inventor in record.get("biblio", {})
                                               .get("parties", {})
                                               .get("inventors", [])
                        if inventor.get("extracted_name", {}).get("value")
                    ],
                    "applicants": [
                        {
                            "organization": applicant.get("extracted_name", {}).get("value")
                        }
                        for applicant in record.get("biblio", {})
                                               .get("parties", {})
                                               .get("applicants", [])
                        if applicant.get("extracted_name", {}).get("value")
                    ]
                }

                patent_date = transformed_record.get("patent_date")
                if patent_date:
                    try:
                        # Validate date and extract YYYY-MM
                        month = datetime.strptime(patent_date, "%Y-%m-%d").strftime("%Y-%m")
                        # Check if the year is 2024
                        if month.startswith("2024"):
                            if month not in monthly_data:
                                monthly_data[month] = []
                            monthly_data[month].append(transformed_record)
                    except ValueError:
                        logging.warning(f"Invalid date format for record: {patent_date}")

        else:
            logging.error(f"Failed to fetch data for branch '{branch_name}'. "
                          f"Status: {response.status_code}, Response: {response.text}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed for branch '{branch_name}': {e}")
        

# Querying for every branch
for branch_name, query_string in branches_data.items():
    process_branch(branch_name, query_string)

# Write data to separate JSON files by month
for month, data in monthly_data.items():
    # Convert the YYYY-MM format to the desired 'YYYY_MMM' format for filename
    month_str = datetime.strptime(month, "%Y-%m").strftime("%Y_%b")
    output_file = f"patents_{month_str}.json"
    try:
        with open(output_file, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4, ensure_ascii=False)
        logging.info(f"Data for {month_str} saved to {output_file}")
    except IOError as e:
        logging.error(f"Failed to write data for {month_str} to file: {e}")

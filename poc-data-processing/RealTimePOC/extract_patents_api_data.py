import requests
import json

# Define the API URL and token
API_URL = "https://api.lens.org/patent/search"
TOKEN = "A5NTTH4NTeEQHU3h63k3J2Fo3POHxbei6bSOGyU9fdGXl8RAVyZi"

# Define branches and their corresponding query strings
branches_data = {
    "Artificial Intelligence": "class_cpc.symbol:(G06N OR G06F19\\/00 OR G06F17\\/27 OR G06F17\\/28 OR G10L15\\/22 OR G06T OR G06K9\\/00)",
    "Data Science and Analytics": "class_cpc.symbol:(G06F18\\/20 OR G06F18\\/22 OR G06F16\\/10 OR G06F16\\/20 OR G06F16\\/30 OR G06F16\\/40)",
    "Networking and Distributed Systems": "class_cpc.symbol:(H04L29\\/08 OR H04L12\\/16 OR H04L12\\/22 OR H04L12\\/26)",
    "Software Development and Security": "class_cpc.symbol:(G06F21\\/00 OR G06F21\\/10 OR G06F21\\/12 OR G06F21\\/30 OR G06F21\\/40)",
    "Advanced Computing Technologies": "class_cpc.symbol:(G06F9\\/50 OR G06F9\\/54 OR G06F9\\/30 OR G06F15\\/16)"
}

# Define the function to fetch and process data for a branch
def process_branch(branch_name, query_string):
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
        raw_data = response.json().get("data", [])
        transformed_data = []

        # Process each record in the "data" array
        for record in raw_data:
            # Safely extract the nested claims field
            claims = record.get("claims", [])
            nested_claims = [claim.get("claims", []) for claim in claims]
            total_num_claims = sum(len(claim_group) for claim_group in nested_claims)

            # Map fields to the desired structure
            transformed_record = {
                "patent_id": record.get("doc_number", None),  # Map doc_number to patent_id
                "num_claims": total_num_claims,  # Count the length of nested claims
                "patent_date": record.get("date_published", None),  # Map date_published to patent_date
                "branch": branch_name,  # Use the branch name
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
        output_file = f"{branch_name.replace(' ', '_').lower()}_patents.json"
        with open(output_file, "w", encoding="utf-8") as file:
            json.dump(transformed_data, file, indent=4, ensure_ascii=False)

        print(f"Data for branch '{branch_name}' has been saved to {output_file}")
    else:
        print(f"Error for branch '{branch_name}': {response.status_code}, {response.text}")

# Loop through each branch and process its data
for branch_name, query_string in branches_data.items():
    process_branch(branch_name, query_string)

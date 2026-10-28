"""Extract proof-of-address fields (name, address, issue date) from a utility bill
or bank statement using Textract AnalyzeDocument with Queries."""
import boto3

textract = boto3.client("textract")
QUERIES = [
    {"Text": "What is the full name on the document?", "Alias": "name"},
    {"Text": "What is the full address?", "Alias": "address"},
    {"Text": "What is the statement or issue date?", "Alias": "issue_date"},
]

def lambda_handler(event, context):
    resp = textract.analyze_document(
        Document={"S3Object": {"Bucket": event["bucket"], "Name": event["address_doc_key"]}},
        FeatureTypes=["QUERIES"],
        QueriesConfig={"Queries": QUERIES},
    )
    blocks = {b["Id"]: b for b in resp["Blocks"]}
    out = {}
    for b in resp["Blocks"]:
        if b["BlockType"] == "QUERY":
            alias = b.get("Query", {}).get("Alias")
            ans = ""
            for rel in b.get("Relationships", []):
                if rel["Type"] == "ANSWER":
                    for aid in rel["Ids"]:
                        ans = blocks.get(aid, {}).get("Text", "")
            out[alias] = ans
    return {"address_fields": out}

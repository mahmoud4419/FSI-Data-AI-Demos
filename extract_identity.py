"""Extract identity fields from an ID document using Amazon Textract AnalyzeID.

Works for passports, Emirates ID, and driver licenses. Returns a flat dict of fields
plus the raw confidence scores so the reasoning step can weigh them.
"""
import boto3

textract = boto3.client("textract")

def lambda_handler(event, context):
    # event: { "bucket": ..., "id_doc_key": ..., "kyc_id": ... }
    resp = textract.analyze_id(DocumentPages=[{"S3Object": {"Bucket": event["bucket"], "Name": event["id_doc_key"]}}])
    fields = {}
    for doc in resp.get("IdentityDocuments", []):
        for f in doc.get("IdentityDocumentFields", []):
            key = f.get("Type", {}).get("Text")
            val = f.get("ValueDetection", {})
            if key:
                fields[key] = {"value": val.get("Text"), "confidence": round(val.get("Confidence", 0), 2)}
    return {"kyc_id": event["kyc_id"], "fields": fields}

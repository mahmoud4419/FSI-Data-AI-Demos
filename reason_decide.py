"""Reconcile extracted fields and face-match result with Amazon Bedrock, and return a
decision (AUTO_APPROVE / MANUAL_REVIEW / REJECT) with a human-readable rationale.

The model only reasons over the facts passed in. Final KYC approval stays with the bank's
controls and a human reviewer for anything below the auto-approve bar.
"""
import json, os
import boto3

bedrock = boto3.client("bedrock-runtime")
MODEL_ID = os.environ.get("MODEL_ID", "anthropic.claude-3-5-sonnet-20240620-v1:0")

SYSTEM = """You are a KYC verification assistant for a UAE retail bank.
You receive extracted ID fields, proof-of-address fields, and a face-match score.
Check: (1) ID not expired, (2) name on ID matches name on address doc (allow transliteration
variants of Arabic names), (3) face match >= 90, (4) all key fields present with high confidence.
Decide AUTO_APPROVE only if all pass with high confidence; REJECT on a clear failure
(expired ID, face mismatch); otherwise MANUAL_REVIEW. Return ONLY JSON:
{"outcome":"AUTO_APPROVE|MANUAL_REVIEW|REJECT","confidence":0-100,"reasons":["..."],"rationale":"one paragraph for the audit log"}"""

def lambda_handler(event, context):
    facts = {
        "identity": event.get("identity", {}).get("Payload", event.get("identity")),
        "checks": event.get("checks", []),
    }
    resp = bedrock.converse(
        modelId=MODEL_ID,
        system=[{"text": SYSTEM}],
        messages=[{"role": "user", "content": [{"text": json.dumps(facts, default=str)}]}],
        inferenceConfig={"maxTokens": 800, "temperature": 0.1},
    )
    text = resp["output"]["message"]["content"][0]["text"]
    try:
        return json.loads(text[text.find("{"): text.rfind("}") + 1])
    except ValueError:
        return {"outcome": "MANUAL_REVIEW", "confidence": 0, "reasons": ["unparseable model output"], "rationale": text[:500]}

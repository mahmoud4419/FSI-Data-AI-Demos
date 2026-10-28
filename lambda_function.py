"""Fraud Investigation Copilot.

Takes a flagged transaction (from Redshift ML fn_detect_fraud), optionally pulls the
customer's recent history from Redshift via the Data API, and asks Amazon Bedrock to
write an investigation narrative plus a draft Suspicious Activity Report (SAR) as JSON.
"""
import json, os, time
import boto3

MODEL_ID = os.environ.get("MODEL_ID", "anthropic.claude-3-5-sonnet-20240620-v1:0")
WORKGROUP = os.environ.get("REDSHIFT_WORKGROUP")      # Redshift Serverless workgroup (optional)
DATABASE = os.environ.get("REDSHIFT_DATABASE", "dev")

bedrock = boto3.client("bedrock-runtime")
rsd = boto3.client("redshift-data")

with open(os.path.join(os.path.dirname(__file__), "prompt.txt")) as f:
    SYSTEM_PROMPT = f.read()


def recent_history(account_id, limit=20):
    """Last N transactions for the account from the streaming MV. Skipped if no workgroup."""
    if not WORKGROUP:
        return []
    sql = ("SELECT txn_timestamp, txn_type, amount, currency, channel, location "
           "FROM banking.mv_live_transactions WHERE account_id = :acct "
           "ORDER BY approximate_arrival_timestamp DESC LIMIT " + str(int(limit)))
    q = rsd.execute_statement(WorkgroupName=WORKGROUP, Database=DATABASE, Sql=sql,
                              Parameters=[{"name": "acct", "value": str(account_id)}])
    while True:
        d = rsd.describe_statement(Id=q["Id"])
        if d["Status"] in ("FINISHED", "FAILED", "ABORTED"):
            break
        time.sleep(0.5)
    if d["Status"] != "FINISHED" or not d.get("HasResultSet"):
        return []
    res = rsd.get_statement_result(Id=q["Id"])
    cols = [c["name"] for c in res["ColumnMetadata"]]
    return [dict(zip(cols, [list(v.values())[0] for v in row])) for row in res["Records"]]


def lambda_handler(event, context):
    if isinstance(event.get("body"), str):          # Lambda Function URL / API Gateway
        event = json.loads(event["body"] or "{}")
    alert = event.get("alert", event)
    history = event.get("history") or recent_history(alert.get("account_id"))
    user_msg = ("Flagged transaction:\n" + json.dumps(alert, indent=2) +
                "\n\nRecent account history:\n" + json.dumps(history, indent=2))
    resp = bedrock.converse(
        modelId=MODEL_ID,
        system=[{"text": SYSTEM_PROMPT}],
        messages=[{"role": "user", "content": [{"text": user_msg}]}],
        inferenceConfig={"maxTokens": 1500, "temperature": 0.2},
    )
    text = resp["output"]["message"]["content"][0]["text"]
    try:
        case = json.loads(text[text.find("{"): text.rfind("}") + 1])
    except ValueError:
        case = {"raw_output": text}
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": json.dumps(case)}


if __name__ == "__main__":  # local test: python lambda_function.py
    with open(os.path.join(os.path.dirname(__file__), "sample_event.json")) as f:
        print(json.dumps(json.loads(lambda_handler(json.load(f), None)["body"]), indent=2))

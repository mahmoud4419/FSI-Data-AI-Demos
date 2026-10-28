# KYC Document Verification Pipeline (Textract + Rekognition + Bedrock)

Automated Know Your Customer (KYC) verification: extract ID fields, match the customer's
selfie to the ID photo, read the proof-of-address document, and let Amazon Bedrock reconcile
everything into a decision with a written rationale for the audit log.

```
Upload (ID + selfie + address doc) in S3
   └─ Step Functions
        ├─ ExtractIdentity   : Textract AnalyzeID (passport / Emirates ID / licence)
        ├─ ParallelChecks
        │    ├─ FaceMatch     : Rekognition CompareFaces (selfie vs ID photo)
        │    └─ ExtractAddress: Textract AnalyzeDocument (Queries) on the bill
        ├─ ReasonAndDecide   : Bedrock reconciles fields + scores -> decision + rationale
        └─ Route: AUTO_APPROVE -> audit | MANUAL_REVIEW -> SNS | REJECT -> audit
```

## Files
| File | Purpose |
|---|---|
| `state_machine.asl.json` | Step Functions definition (placeholders like `${ExtractIdFn}` are resolved at deploy) |
| `extract_identity.py` | Textract AnalyzeID |
| `face_match.py` | Rekognition CompareFaces (add Face Liveness for anti-spoofing) |
| `extract_address.py` | Textract AnalyzeDocument with Queries |
| `reason_decide.py` | Bedrock Converse: reconcile + decide |
| `sample_event.json` | Example input (Emirates ID + selfie + DEWA bill) |
| `iam_policy.json` | Permissions for the Lambda/Step Functions roles |

## Latest GenAI option
Instead of wiring Textract and the reasoning glue yourself, **Amazon Bedrock Data Automation (BDA)**
can run IDP over a mixed document packet (ID + bill + form) in one managed job and return
normalized output. Swap the `ExtractIdentity` / `ExtractAddress` steps for a single BDA invocation
when you want less glue code.

## Processing time
The pipeline runs in seconds to a couple of minutes per applicant. A "2 days -> 5 minutes" style
improvement comes mostly from removing manual queues and handoffs, not from the API latency — present
it as a representative automation gain, measured against the bank's own current process.

## Compliance (CBUAE) — design notes, not a certification
- **Data residency**: deploy in `me-central-1` (UAE). Before committing, confirm Rekognition Face
  Liveness and your chosen Bedrock model are available there (region availability shifts; some Bedrock
  models need a cross-region inference profile).
- **Audit trail**: every decision writes a DynamoDB record plus the Bedrock-generated rationale.
- **Human-in-the-loop**: anything below the auto-approve bar goes to a manual-review queue.
- Treat "supports CBUAE KYC/AML requirements" as a design goal. Compliance is achieved by the bank's
  controls and sign-off, not by the AWS services themselves. Verify current CBUAE eKYC rules before relying on this.

## Disclaimer
Illustrative demo code. Not production-ready and not legal/compliance advice.
https://github.com/mahmoud4419/FSI-Data-AI-Demos/blob/main/RetailBankingDataAI.png

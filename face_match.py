"""Compare the selfie against the ID photo with Amazon Rekognition CompareFaces.

For production anti-spoofing, add a Rekognition Face Liveness session (CreateFaceLivenessSession)
on the client side before this step — CompareFaces alone does not prove liveness.
"""
import boto3

rek = boto3.client("rekognition")
SIMILARITY_THRESHOLD = 90.0

def lambda_handler(event, context):
    b = event["bucket"]
    resp = rek.compare_faces(
        SourceImage={"S3Object": {"Bucket": b, "Name": event["selfie_key"]}},
        TargetImage={"S3Object": {"Bucket": b, "Name": event["id_doc_key"]}},
        SimilarityThreshold=0,
    )
    matches = resp.get("FaceMatches", [])
    best = max((m["Similarity"] for m in matches), default=0.0)
    return {"face_similarity": round(best, 2), "face_match": best >= SIMILARITY_THRESHOLD}

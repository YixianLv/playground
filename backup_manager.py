from google.oauth2 import service_account
import googleapiclient.discovery

# Construct service account credentials using the service account key
# file.
credentials = service_account.Credentials.from_service_account_file(
    'devops-assessment-yixian.json')

# Explicitly pass the credentials to the client library.
storage_client = googleapiclient.discovery.build(
    'storage', 'v1', credentials=credentials)

# Make an authenticated API request
buckets = storage_client.buckets().list(project="devops-assessment-yixian").execute()
print(buckets)

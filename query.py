from google.cloud import bigquery

client = bigquery.Client()
query_job = client.query("""
SELECT * FROM `admintome-bigdata-test.www_logs.access_logs`
""")
results = query_job.result()
for row in results:
    print("Request: {}".format(row.request()))
if results.total_rows == 0:
    print("No logs returned")

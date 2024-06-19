import json 
from datetime import date, timedelta
from generate_web_logs import generate_web_logs
from upload_to_s3 import upload_to_s3
import io
import csv

start_date = date(2024,5,19)
end_date = date.today()

def lambda_handler(event, context):
    data = []
    for i in range((end_date - start_date).days + 1):
        curr_date = start_date + timedelta(days = i)
        print(curr_date)

        data.append(generate_web_logs(curr_date))

    flattened_data = [item for subdata in data for item in subdata]
    print(flattened_data)


    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=flattened_data[0].keys())
    writer.writeheader()
    writer.writerows(flattened_data)
    csv_buffer.seek(0)
    csv_data = csv_buffer.getvalue()

    csv_bytes = csv_data.encode("utf-8") 

    csv_buffer = io.BytesIO(csv_bytes) 

    upload_to_s3('web_logs_monthly.csv',csv_buffer)


    return {
        'statusCode':200,
        'body':f"Web logs data generated successfully !!!"
    }

    

lambda_handler(1,1)
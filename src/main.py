from sodapy import Socrata
import requests
from requests.auth import HTTPBasicAuth
import json
import argparse
import sys
import os

parser = argparse.ArgumentParser(description='Fire Incident Dispatch Data')
parser.add_argument('--page_size', type=int, help='how many rows to get per page', required=True)
parser.add_argument('--numpages',default=10, type=int, help='how many pages to get in total')
args = parser.parse_args(sys.argv[1:])
print(args)

DATASET_ID=os.environ["DATASET_ID"]
APP_TOKEN=os.environ["APP_TOKEN"]
ES_HOST=os.environ["ES_HOST"]
ES_USERNAME=os.environ["ES_USERNAME"]
ES_PASSWORD=os.environ["ES_PASSWORD"]
INDEX_NAME=os.environ["INDEX_NAME"]

if __name__ == '__main__': 

    try:
        resp = requests.put(f"{ES_HOST}/{INDEX_NAME}", auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            json={
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                "mappings": {
                    "properties": {
                        "starfire_incident_id": {"type": "keyword"},
                        "incident_datetime": {"type": "date"},
                        "alarm_box_borough": {"type": "keyword"},
                        "alarm_box_location": {"type": "keyword"},
                        "zipcode": {"type": "keyword"},
                        "incident_classification": {"type": "keyword"},
                        "ladders_assigned_quantity": {"type": "float"},
                    }
                },
            }
        )
        resp.raise_for_status()
        #print(resp.json())
        
    except Exception as e:
        print("Index already exists! Skipping")    
    
    client = Socrata("data.cityofnewyork.us", APP_TOKEN, timeout=10000)

    numpages = args.numpages
    if numpages == False:
        numpages = total_rows/args.page_size
    else:
        numpages = numpages
    
    for page in range(numpages):
        rows= client.get(DATASET_ID, limit= args.page_size, offset= page*args.page_size, where = "starfire_incident_id IS NOT NULL and incident_datetime IS NOT NULL")
        es_rows=[]
        for row in rows:
            try:
                es_row = {}
                es_row["starfire_incident_id"] = row["starfire_incident_id"]
                es_row["incident_datetime"] = row["incident_datetime"]
                es_row["alarm_box_borough"] = row["alarm_box_borough"]
                es_row["alarm_box_location"] = row["alarm_box_location"]
                es_row["incident_classification"] = row["incident_classification"]
                es_row["zipcode"] = row["zipcode"]
                es_row["ladders_assigned_quantity"] = float(row["ladders_assigned_quantity"]) 
                
            except Exception as e:
                print (f"Error!: {e}, skipping row: {row}")
                continue
            
            es_rows.append(es_row)
        
        bulk_upload_data = ""
        for line in es_rows:
            print(f'Handling row {line["starfire_incident_id"]}')
            action = '{"index": {"_index": "' + INDEX_NAME + '", "_type": "_doc", "_id": "' + line["starfire_incident_id"] + '"}}'
            data = json.dumps(line)
            bulk_upload_data += f"{action}\n"
            bulk_upload_data += f"{data}\n"
        #print (bulk_upload_data)
        
        try:
            resp = requests.post(f"{ES_HOST}/_bulk",
                        data=bulk_upload_data,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers = {"Content-Type": "application/x-ndjson"})
            resp.raise_for_status()
            print ('Done')
                
        except Exception as e:
            print(f"Failed to insert in ES: {e}")

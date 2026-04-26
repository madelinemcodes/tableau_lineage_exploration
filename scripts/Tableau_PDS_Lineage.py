import json
import pandas as pd
from tableau_api_lib import TableauServerConnection
import datetime
import os
import shutil
from pathlib import Path
import tableauserverclient as TSC
import subprocess
from dotenv import load_dotenv
load_dotenv()

time_now = datetime.datetime.now()

config = {
    "tableau_prod": {
        "server": os.environ.get("TABLEAU_SERVER"),
        "api_version": "3.28",
        "personal_access_token_name": os.environ.get("TABLEAU_PAT_NAME"),
        "personal_access_token_secret": os.environ.get("TABLEAU_PAT_SECRET"),
        "site_name": os.environ.get("TABLEAU_SITE"),
        "site_url": os.environ.get("TABLEAU_SITE_URL"),
    }
}

conn = TableauServerConnection(config, env="tableau_prod")
conn.sign_in()

#Start with Published Data Source Lineage
pds_metadata = 'pds_lineage.txt'

with open (pds_metadata,'r') as file:
    data = file.read()

pds_response = conn.metadata_graphql_query(query=data)

def flatten_datasource(datasource):
    base_info = {
        'upstream_type': 'Datasource',
        'upstream_luid': datasource.get('luid',''),
        'upstream_name': datasource.get('name',''),
        'upstream_datasource_type': 'Published'
    }

    flattened_data = []

    #If no downstream entities, add a row with placeholder values
    if not (datasource.get('downstreamDatasources') or
            datasource.get('downstreamWorkbooks') or
            datasource.get('downstreamFlows')):
        row = base_info.copy()
        row.update({
            'downstream_type': '',
            'downstream_luid':'',
            'downstream_name':''
        })
        flattened_data.append(row)

    #Process downstreamDatasources
    for downstream in datasource.get('downstreamDatasources',[]):
        row = base_info.copy()
        row.update({
            'downstream_type': 'Datasource',
            'downstream_luid': downstream.get('luid',''),
            'downstream_name': downstream.get('name','')
        })
        flattened_data.append(row)

    #Process downstreamWorkbooks
    for downstream in datasource.get('downstreamWorkbooks',[]):
        row = base_info.copy()
        row.update({
            'downstream_type': 'Workbook',
            'downstream_luid': downstream.get('luid',''),
            'downstream_name': downstream.get('name','')
        })
        flattened_data.append(row)

    #Process downstreamFlows    
    for downstream in datasource.get('downstreamFlows',[]):
        row = base_info.copy()
        row.update({
            'downstream_type': 'Flow',
            'downstream_luid': downstream.get('luid',''),
            'downstream_name': downstream.get('name','')
        })
        flattened_data.append(row)

    return flattened_data

#Process JSON
def process_json_to_dataframe(json_data):
    data = json.loads(json_data)
    datasources = data['data']['publishedDatasources']
    all_flattened_data = []

    for datasource in datasources:
        all_flattened_data.extend(flatten_datasource(datasource))

    df = pd.DataFrame(all_flattened_data)

    all_columns = [
        'upstream_type', 'upstream_luid', 'upstream_name', 'upstream_datasource_type', 'downstream_type', 'downstream_luid', 'downstream_name'
        ]

    for col in all_columns:
        if col not in df.columns:
            df[col] = pd.NA

    df= df[all_columns]
    return df

json_output = pds_response.json()
json_string = json.dumps(json_output)
pds_df = process_json_to_dataframe(json_string)

#Continue with Embedded Data Source Lineage
eds_metadata = 'eds_lineage.txt'

with open (eds_metadata,'r') as file:
    data = file.read()

eds_response = conn.metadata_graphql_query(query=data)

def flatten_datasource(embedded):
    base_info = {
        'upstream_type': 'Datasource',
        'downstream_type': 'Workbook',
        }
    wb = embedded.get("workbook",{}) or []
    wb_luid = wb.get("luid","")
    wb_name = wb.get("name","")

    flattened_data=[]

    parents=embedded.get('parentPublishedDatasources',[]) or []

    if not parents:
        row = base_info.copy()
        row.update({
            "upstream_luid":embedded.get("id",""),
            "upstream_name":embedded.get("name",""),
            "upstream_datasource_type":"Embedded",
            "downstream_luid": wb_luid,
            "downstream_name": wb_name,
        })
        flattened_data.append(row)
        return flattened_data

    for parent in parents:
        row = base_info.copy()
        row.update({
            "upstream_luid": parent.get("luid",""),
            "upstream_name": parent.get("name",""),
            "upstream_datasource_type": "Published",
            "downstream_luid": wb_luid,
            "downstream_name": wb_name,
        })
        flattened_data.append(row)
    return flattened_data

def process_json_to_dataframe(json_data):
    data = json.loads(json_data)
    embeddedlist = data['data']['embeddedDatasources']
    all_flattened_data = []

    for embedded in embeddedlist:
        all_flattened_data.extend(flatten_datasource(embedded))


    embedded_df = pd.DataFrame(all_flattened_data)
    all_columns = [
        'upstream_type', 'upstream_luid', 'upstream_name', 'upstream_datasource_type', 'downstream_type', 'downstream_luid', 'downstream_name']

    for col in all_columns:
        if col not in embedded_df.columns:
            embedded_df[col]=pd.NA

    embedded_df = embedded_df[all_columns]

    return embedded_df

json_output = eds_response.json()
json_string = json.dumps(json_output)
eds_df = process_json_to_dataframe(json_string)

#COMBINE THEM and DEDUPE
union_df = pd.concat([pds_df, eds_df],ignore_index=True)
union_df = union_df.drop_duplicates().reset_index(drop=True)
union_df['runtime'] = time_now


output_dir = os.path.join(os.path.dirname(__file__), '..', 'output')
os.makedirs(output_dir, exist_ok=True)
file_name = f'Tableau_DS_Lineage_{time_now}.csv'
output_path = os.path.join(output_dir, file_name)
union_df.to_csv(output_path, index=False)
print(f"File written to: {output_path}")


from typing import Any, Dict, List, Union
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import json
import psycopg2 
import uuid
from fastapi.middleware.cors import CORSMiddleware
import httpx
from fastapi.responses import JSONResponse

app = FastAPI()

# CORS configuration to allow all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class TokenData(BaseModel):
    token: str
    sheetId: str
    tabId: int
    email: str

class CreateEndpointRequest(BaseModel):
    sheetId: str
    tabId: int
    sheetName : str

def collect_keys(data, level=0, keys_dict=None, prevkey="", colchanges=[]):
    if keys_dict is None:
        keys_dict = [[]]
        
    # Ensure keys_dict has enough levels
    while level >= len(keys_dict):
        if level == 0:
            keys_dict.append([])
        else:
            keys_dict.append([''] * len(keys_dict[level - 1]))

    # Ensure current level has the correct number of columns
    if level > 0 and len(keys_dict[level]) < len(keys_dict[level - 1]):
        y = len(keys_dict[level])
        while y < len(keys_dict[level - 1]):
            keys_dict[level].append('')
            y += 1

    if isinstance(data, dict):
        for key, value in data.items():
            full_key = prevkey + "char$tGPT" + key if prevkey else key

            if level > 0:
                if full_key not in keys_dict[level]:
                    if prevkey:
                        try:
                            rev_index = keys_dict[level - 1][::-1].index(prevkey)
                            y = len(keys_dict[level - 1]) - rev_index - 1
                        except ValueError:
                            y = len(keys_dict[level - 1])
                    else:
                        y = len(keys_dict[level])
                        
                    if 'char$tGPT'.join(keys_dict[level][y].split('char$tGPT')[:-1]) == prevkey:
                        y += 1
                    keys_dict[level].insert(y, full_key)
                    if not isinstance(value, (dict, list)):
                        colchanges.append(y)

                    if len(keys_dict[level]) > y:
                        level2 = level
                        oldkey = full_key
                        while level2 > 0:
                            oldkey = 'char$tGPT'.join(oldkey.split('char$tGPT')[:-1])
                            rev_index = keys_dict[level2 - 1][::-1].index(oldkey)
                            index = len(keys_dict[level2 - 1]) - rev_index - 1
                            m = 0
                            times = 0
                            while m < len(keys_dict[level2]):
                                if 'char$tGPT'.join(keys_dict[level2][m].split('char$tGPT')[:-1]) == oldkey:
                                    times += 1
                                m += 1
                            if times > keys_dict[level2 - 1].count(oldkey):
                                keys_dict[level2 - 1].insert(index, oldkey)
                            level2 -= 1

                        level2 = level
                        if not isinstance(value, (dict, list)):
                            while level2 < len(keys_dict) - 1:
                                keys_dict[level2 + 1].insert(y, '')
                                level2 += 1

                if isinstance(value, dict):
                    collect_keys(value, level + 1, keys_dict, full_key, colchanges)

                elif isinstance(value, list):
                    for j in range(len(value)):
                        if isinstance(value[j], dict):
                            collect_keys(value[j], level + 1, keys_dict, full_key, colchanges)

            else:
                if full_key not in keys_dict[level]:
                    keys_dict[level].append(full_key)

                if isinstance(value, dict):
                    collect_keys(value, level + 1, keys_dict, full_key, colchanges)

                elif isinstance(value, list):
                    for j in range(len(value)):
                        if isinstance(value[j], dict):
                            collect_keys(value[j], level + 1, keys_dict, full_key, colchanges)

    return [keys_dict, colchanges]


def fill_rows(data, level=0, keys_dict=[],row=[],rowlevel=0,prevkey=""):
    
    if row == []:
        row.append(['']*len(keys_dict[0]))
    
    if rowlevel>(len(row)-1):
        row.append(['']*len(keys_dict[0]))
    
    if isinstance(data, dict):
        for key, value in data.items():
            if prevkey!= "":
                key = prevkey+"char$tGPT"+key
            if key in keys_dict[level]:
                if isinstance(value,dict):
                    fill_rows(value,level+1,keys_dict,row,rowlevel,key)
            
                elif isinstance(value,list):                                                                           
                    pos = len(row)
                    if not isinstance(value[0],dict):   
                        index = keys_dict[level].index(key)
                        row[rowlevel][index] = repr(value)  
                    else:        
                        for j in range(0,len(value)):
                            if isinstance(value[j],dict):                        
                                fill_rows(value[j],level+1,keys_dict,row,pos,key)
                                pos = len(row)
                    
                else:
                    index = keys_dict[level].index(key)
                    row[rowlevel][index] = str(value)
    else:
        index = keys_dict[level].index(key)
        row[rowlevel][index] = str(value)

    return row


def format_keys(keys_dict):
    
    max_len = max(len(keys) for keys in keys_dict)
    formatted_keys = []

    for keys in keys_dict:
        while len(keys) < max_len:
            keys.append('')
        formatted_keys.append(keys)
    
    keys = formatted_keys

    y1 = 0
    while y1<len(keys[0]):
        y2 = 0
        counter = 0
        while y2<len(keys):
            if keys[y2][y1]=='':
                counter = counter+1
            y2 = y2 + 1

        if counter==len(keys):
            keys = [[row[i] for i in range(len(row)) if i != y1] for row in keys]
        else:
            y1 = y1 + 1
    return keys 

def getback(keys):
    y1 = 0
    while y1<len(keys):
        y2 = 0
        while y2<len(keys[y1]):
            if keys[y1][y2]!='':
                keys[y1][y2] = keys[y1][y2].split('char$tGPT')[-1]
            y2 = y2 + 1
        y1 = y1 + 1

    return keys    

def merge(keys,keys1):
    requests = []
    requests.append({
        "updateCells": {
            "range": {
                "sheetId": 0,
                "startRowIndex": 0,
                "endRowIndex": len(keys1),
                "startColumnIndex": 0,
                "endColumnIndex": len(keys1[0])
            },
            "rows": [{
                "values": [
                {"userEnteredValue": {"stringValue": str(cell)}} for cell in row
            ]} for row in keys1
        ],
        "fields": "userEnteredValue"
        }
    })
    y1 = 0
    while y1<len(keys):
        y2 = 0
        count=1
        while y2<len(keys[y1]):
            if y2>0:
                if keys[y1][y2]==keys[y1][y2-1] and keys[y1][y2]!='':
                    count = count+1
                else:
                    if count>1:
                        requests.append({
                            'mergeCells': {
                                'range': {
                                    'sheetId': 0,
                                    'startRowIndex': y1,
                                    'endRowIndex': y1+1,
                                    'startColumnIndex': y2-count,
                                    'endColumnIndex': y2
                                    },
                                'mergeType': 'MERGE_ALL'  # Other options include 'MERGE_COLUMNS', 'MERGE_ROWS'
                                }
                            })
                    count = 1
            y2 = y2 + 1

        if count>1:
            requests.append({
                'mergeCells': {
                    'range': {
                        'sheetId': 0,
                        'startRowIndex': y1,
                        'endRowIndex': y1+1,
                        'startColumnIndex': y2-count,
                        'endColumnIndex': y2
                        },
                    'mergeType': 'MERGE_ALL'  # Other options include 'MERGE_COLUMNS', 'MERGE_ROWS'
                    }
                })
            
        y1 = y1 + 1
    y1 = 0
    while y1<len(keys[0]):
        count = 1
        y2 = 0
        while y2<len(keys):
            if y2>0:
                if keys[y2][y1]=='':
                    count = count+1
                else:
                    if count>1:
                        requests.append({
                            'mergeCells': {
                                'range': {
                                    'sheetId': 0,
                                    'startRowIndex': y2-count,
                                    'endRowIndex': y2,
                                    'startColumnIndex': y1,
                                    'endColumnIndex': y1+1
                                    },
                                'mergeType': 'MERGE_ALL'  # Other options include 'MERGE_COLUMNS', 'MERGE_ROWS'
                                }
                            })
                    count = 1
            y2 = y2 + 1
        
        if count>1:
            requests.append({
                'mergeCells': {
                    'range': {
                        'sheetId': 0,
                        'startRowIndex': y2-count,
                        'endRowIndex': y2,
                        'startColumnIndex': y1,
                        'endColumnIndex': y1+1
                        },
                    'mergeType': 'MERGE_ALL'  # Other options include 'MERGE_COLUMNS', 'MERGE_ROWS'
                    }
                })
        y1 = y1 + 1

    return requests            

def value_merge(rows,pos):
    requests = []
    requests.append({
        "updateCells": {
            "range": {
                "sheetId": 0,
                "startRowIndex": pos,
                "endRowIndex": pos+len(rows),
                "startColumnIndex": 0,
                "endColumnIndex": len(rows[0])
            },
            "rows": [{
                "values": [
                {"userEnteredValue": {"stringValue": str(cell)}} for cell in row
            ]} for row in rows
        ],  
        "fields": "userEnteredValue"
        }
    })

    """y1 = 0
    while y1<len(rows[0]):
        count = 1
        y2 = 0
        while y2<len(rows):
            if y2>0:
                if rows[y2][y1]=='':
                    count = count+1
                else:
                    if count>1:
                        requests.append({
                            'mergeCells': {
                                'range': {
                                    'sheetId': 0,
                                    'startRowIndex': pos+y2-count,
                                    'endRowIndex': pos+y2,
                                    'startColumnIndex': y1,
                                    'endColumnIndex': y1+1
                                    },
                                'mergeType': 'MERGE_ALL'  # Other options include 'MERGE_COLUMNS', 'MERGE_ROWS'
                                }
                            })
                    count = 1
            y2 = y2 + 1
        
        if count>1:
            requests.append({
                'mergeCells': {
                    'range': {
                        'sheetId': 0,
                        'startRowIndex': pos+y2-count,
                        'endRowIndex': pos+y2,
                        'startColumnIndex': y1,
                        'endColumnIndex': y1+1
                        },
                    'mergeType': 'MERGE_ALL'  # Other options include 'MERGE_COLUMNS', 'MERGE_ROWS'
                    }
                })
        y1 = y1 + 1"""

    return requests   

@app.post("/sendtoken", response_model=None)
async def receive_token(data: TokenData):
    print("hello")
    insert_query = """INSERT INTO oauth_token ("token","sheetId","utcTime") VALUES (%s, %s, %s) ON CONFLICT ("sheetId")  DO UPDATE SET "token" = EXCLUDED."token", "utcTime" = EXCLUDED."utcTime";"""
    data_query = (data.token, data.sheetId, datetime.now())
    conn_sq = psycopg2.connect("postgresql://retool:yosc9BrPx5Lw@ep-silent-hill-00541089.us-west-2.retooldb.com/retool?sslmode=require")
    cur_sq = conn_sq.cursor()
    cur_sq.execute(insert_query, data_query)
    conn_sq.commit()
    
    # Check if 'Hits' sheet exists, if not, create it
    creds = Credentials(token=data.token)
    service = build('sheets', 'v4', credentials=creds)
    sheet_metadata = service.spreadsheets().get(spreadsheetId=data.sheetId).execute()
    sheets = sheet_metadata.get('sheets', '')
    sheet_names = [sheet['properties']['title'] for sheet in sheets]

    if 'Hits' not in sheet_names:
        requests = [{
            'addSheet': {
                'properties': {
                    'title': 'Hits',
                    'gridProperties': {
                        'rowCount': 100,
                        'columnCount': 5  # Decrease column count to 5
                    }
                }
            }
        }]
        body = {
            'requests': requests
        }
        service.spreadsheets().batchUpdate(spreadsheetId=data.sheetId, body=body).execute()

        # Add headers to the 'Hits' sheet without 'Data' column
        headers = [['URL', 'Hostname', 'User Agent', 'Date/Time', 'Method']]
        range_name = 'Hits!A1'
        body = {
            'values': headers
        }
        service.spreadsheets().values().update(
            spreadsheetId=data.sheetId,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()

    return {"status": "token received and Hits sheet ensured"}

@app.post("/datasink/{param:path}")
async def receive_token(param: str, data: Dict):
    conn = psycopg2.connect("postgresql://retool:yosc9BrPx5Lw@ep-silent-hill-00541089.us-west-2.retooldb.com/retool?sslmode=require")
    cur = conn.cursor()
    query = """SELECT "sheetId", "tabId", "rows" FROM header_structure WHERE "param" = %s;"""
    cur.execute(query, (param,))
    row = cur.fetchone()
    
    if row:
        query = """SELECT "token" FROM oauth_token WHERE "sheetId" = %s;"""
        cur.execute(query, (row[0],))
        token = cur.fetchone()
        
        access_token = token[0]
        creds = Credentials(token=access_token)
        service = build('sheets', 'v4', credentials=creds)
        
        raw_feed = getdata(token[0], row[0], row[1], row[2])
        
        header = raw_feed[0]
        lastrow = raw_feed[1]

        results = collect_keys(data, 0, header, "", [])
        
        cleaned = format_keys(results[0])
        datarow = fill_rows(data, 0, cleaned, [], 0, "")
        cleaned_2 = getback(cleaned.copy())

        # Check if the header length is greater than the existing rows
        if len(cleaned) > int(row[2]):
            requests = [
                {
                    "insertDimension": {
                        "range": {
                            "sheetId": row[1],
                            "dimension": "ROWS",
                            "startIndex": int(row[2]),
                            "endIndex": len(cleaned)
                        },
                        "inheritFromBefore": False  # or True depending on context
                    }
                }
            ]
            
            body = {'requests': requests}
            service.spreadsheets().batchUpdate(spreadsheetId=row[0], body=body).execute()

        # Insert columns if needed
        if len(results[1]) > 0: 
            requests = []
            for j in range(len(results[1])):
                requests.append({
                    "insertRange": {
                        "range": {
                            "sheetId": row[1],
                            "startRowIndex": len(cleaned),
                            "endRowIndex": lastrow + len(cleaned) - int(row[2]) + 1,  
                            "startColumnIndex": results[1][j],  
                            "endColumnIndex": results[1][j] + 1,
                        },
                        "shiftDimension": "COLUMNS" 
                    }
                })
            
            body = {'requests': requests}
            service.spreadsheets().batchUpdate(spreadsheetId=row[0], body=body).execute()
    
        # Merge and write headers
        requests = merge(cleaned, cleaned_2)
        # Write data rows
        requests.append(value_merge(datarow, lastrow + len(cleaned) - int(row[2])))

        # Clear formatting and values in the range
        clear_formatting_request = {
            'requests': [{
                'unmergeCells': {
                    'range': {
                        'sheetId': 0, 
                        "startRowIndex": 0,
                        "endRowIndex": len(cleaned),  
                        "startColumnIndex": 0,  
                        "endColumnIndex": len(cleaned[0])
                    },
                }
            }]
        }
        
        clear_values_request = {
            'requests': [{
                'updateCells': {
                    'range': {
                        'sheetId': 0, 
                        "startRowIndex": 0,
                        "endRowIndex": len(cleaned),  
                        "startColumnIndex": 0,  
                        "endColumnIndex": len(cleaned[0]) 
                    },
                    'fields': 'userEnteredValue'
                }   
            }]
        }
        
        service.spreadsheets().batchUpdate(spreadsheetId=row[0], body=clear_values_request).execute()
        service.spreadsheets().batchUpdate(spreadsheetId=row[0], body=clear_formatting_request).execute()

        body = {'requests': requests}
        service.spreadsheets().batchUpdate(spreadsheetId=row[0], body=body).execute()

        # Update header structure with the new number of rows
        new_row_count = len(results[0])
        query = """UPDATE header_structure SET "rows" = %s WHERE "sheetId" = %s AND "tabId" = %s;"""
        cur.execute(query, (new_row_count, row[0], row[1]))
        conn.commit()
    
    cur.close()
    conn.close()
    return 0


def getdata(token,sheetId,tabId,rows):
    
    access_token = token
    spreadsheet_id = sheetId
    creds = Credentials(token=access_token)
    service = build('sheets', 'v4', credentials=creds)

    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get('sheets', '')
    sheet_name = None
    for sheet in sheets:
        if sheet['properties']['sheetId'] == int(tabId):
            sheet_name = sheet['properties']['title']
            break
    
    if int(rows)!=0:
        range_name = f'{sheet_name}!1:{rows}' 
        range_all = f'{sheet_name}'
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        result_all = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_all).execute()
        values = result.get('values', [])
        values_all = result_all.get('values', [])

        if values!=[]:
            max_len = max(len(keys) for keys in values)
            formatted_keys = []

            for keys in values:
                while len(keys) < max_len:
                    keys.append('')
                formatted_keys.append(keys)
        
            values = formatted_keys

        y1 = 0
        while y1<len(values):
            if y1==0:
                y2 = 0
                while y2<len(values[y1]):   
                    if values[y1][y2]=='' and y2>0:
                        values[y1][y2] = values[y1][y2-1]
                    y2 = y2+1
            else:
                y2 = 0
                flag = values[y1-1][0]
                detect = 0
                while y2<len(values[y1]):
                    if values[y1][y2]!='':
                        detect = 1 
                    if values[y1-1][y2]==flag and values[y1][y2]=='' and y2>0 and detect==1:
                        values[y1][y2] = values[y1][y2-1]
                    else:
                        flag = values[y1-1][y2]
                    y2 = y2 + 1
            y1 = y1 + 1

        y1 = 0
        while y1<len(values):
            y2 = 0
            while y2<len(values[y1]):   
                if y1>0 and values[y1][y2]!='':
                    values[y1][y2] = values[y1-1][y2]+"char$tGPT"+values[y1][y2]
                y2 = y2 + 1
            y1 = y1 + 1
        
    else:
        values = [[]]
        values_all = []

    return [values,len(values_all)]

def generate_unique_url(cur):
    while True:
        endpoint_url = f"{str(uuid.uuid4())}"
        check_query = "SELECT COUNT(*) FROM header_structure WHERE param = %s;"
        cur.execute(check_query, (endpoint_url,))
        if cur.fetchone()[0] == 0:
            return endpoint_url

@app.post("/create-endpoint")
async def create_endpoint(request: CreateEndpointRequest):
    conn = psycopg2.connect("postgresql://retool:yosc9BrPx5Lw@ep-silent-hill-00541089.us-west-2.retooldb.com/retool?sslmode=require")
    cur = conn.cursor()

    # Generate a unique endpoint URL
    endpoint_url = generate_unique_url(cur)

    # Insert new endpoint information into header_structure
    insert_query = """INSERT INTO header_structure ("param", "sheetId", "tabId", "rows") VALUES (%s, %s, %s, %s);"""
    rows = 0  # Initial rows set to 0, this will be updated later
    tab_id = request.tabId  
    data_query = (endpoint_url, request.sheetId, tab_id, rows)
    cur.execute(insert_query, data_query)
    conn.commit()

    cur.close()
    conn.close()

    return {"url": endpoint_url, "sheetId": request.sheetId, "sheetName": request.sheetName}


@app.api_route("/{endpoint_id}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"], response_model=None)
async def handle_webhook(endpoint_id: str, request: Request):
    # Fetch JSON data if present
    if request.method in ["POST", "PUT", "PATCH"]:
        try:
            data = await request.json()
        except:
            data = {}
    else:
        data = {}

    user_agent = request.headers.get('user-agent')
    method = request.method
    hostname = request.client.host
    url = str(request.url)
    hit_time = datetime.now().isoformat()

    # Fetch the token and sheet information based on the endpoint_id
    conn = psycopg2.connect("postgresql://retool:yosc9BrPx5Lw@ep-silent-hill-00541089.us-west-2.retooldb.com/retool?sslmode=require")
    cur = conn.cursor()
    query = """SELECT "sheetId", "tabId" FROM header_structure WHERE "param" = %s;"""
    cur.execute(query, (endpoint_id,))
    row = cur.fetchone()
    
    if not row:
        raise HTTPException(status_code=404, detail="Endpoint not found")

    sheet_id, tab_id = row
    query = """SELECT "token" FROM oauth_token WHERE "sheetId" = %s;"""
    cur.execute(query, (sheet_id,))
    token = cur.fetchone()
    
    if not token:
        raise HTTPException(status_code=404, detail="Token not found")

    access_token = token[0]
    creds = Credentials(token=access_token)
    service = build('sheets', 'v4', credentials=creds)

    # Prepare the hit data without 'data' column
    hit_data = [
        [url, hostname, user_agent, hit_time, method]
    ]

    # Append the data to the 'Hits' tab
    sheet_name = 'Hits'
    range_name = f'{sheet_name}!A1'
    body = {
        'values': hit_data
    }
    
    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=range_name,
        valueInputOption='USER_ENTERED',
        body=body
    ).execute()

    # Forward the request to the second endpoint
    forward_url = f"https://newcode-9d5c.onrender.com/datalink/{endpoint_id}"
    headers = dict(request.headers)

    async with httpx.AsyncClient() as client:
        if request.method == "GET":
            forward_response = await client.get(forward_url, headers=headers)
        elif request.method == "POST":
            forward_response = await client.post(forward_url, json=data, headers=headers)
        elif request.method == "PUT":
            forward_response = await client.put(forward_url, json=data, headers=headers)
        elif request.method == "PATCH":
            forward_response = await client.patch(forward_url, json=data, headers=headers)
        elif request.method == "DELETE":
            forward_response = await client.delete(forward_url, headers=headers)
        else:
            forward_response = JSONResponse(status_code=405, content={"message": "Method not allowed"})

    if forward_response.status_code != 200:
        raise HTTPException(status_code=forward_response.status_code, detail=forward_response.text)

    return {"status": "success", "data": hit_data, "forward_response": forward_response.json()}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
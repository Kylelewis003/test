import os
import requests
import json
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import google.generativeai as genai

# Initialize FastAPI app
app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:18081"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"], 
)

# Configure API key for Google Gemini
os.environ["GOOGLE_API_KEY"] = "AIzaSyCHJABY2VcH3k5xPMqDoFyLEuDDjI7nQLc"
api_key = os.environ.get("GOOGLE_API_KEY")
if api_key is None:
    raise ValueError("API key not found. Please set the GOOGLE_API_KEY environment variable.")
else:
    genai.configure(api_key=api_key)

# Predefined queries related to the selected table
PREDEFINED_QUERIES = {
    "default": [
        {"query": "Show all records from the selected table.", "druid": "SELECT * FROM {table_name}"},
        {"query": "Count the total number of records in the selected table.", "druid": "SELECT COUNT(*) FROM {table_name}"},
        {"query": "Get a limited number of records from the selected table.", "druid": "SELECT * FROM {table_name} LIMIT 10"},
        {"query": "Order records from the selected table.", "druid": "SELECT * FROM {table_name} ORDER BY __time DESC"}
    ]
}

# Function to fetch columns for a given table from Druid
def fetch_columns_from_druid(table_name):
    druid_url = "http://localhost:8888/druid/v2/sql"
    query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    headers = {'Content-Type': 'application/json'}
    payload = {'query': query}
    
    try:
        response = requests.post(druid_url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        if isinstance(response.json(), list):
            return [col['COLUMN_NAME'] for col in response.json() if 'COLUMN_NAME' in col]
        else:
            raise ValueError("Unexpected response format from Druid.")
    except requests.exceptions.HTTPError as http_err:
        raise HTTPException(status_code=400, detail=f"HTTP error occurred: {http_err}")
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")

# Pydantic models for request and response
class NLQueryRequest(BaseModel):
    table_name: str
    user_input: str

class QueryResponse(BaseModel):
    generated_query: str

# Endpoint to fetch columns from Druid
@app.get("/fetch_columns/{table_name}")
async def get_columns(table_name: str):
    columns = fetch_columns_from_druid(table_name)
    return {"columns": columns}

# Endpoint to submit natural language queries
@app.post("/nl_query/", response_model=QueryResponse)
async def nl_query(request: NLQueryRequest):
    table_name = request.table_name
    user_input = request.user_input

    if not user_input.strip():
        raise HTTPException(status_code=400, detail="Please enter a valid query.")

    # Fetch columns for the given table
    columns = fetch_columns_from_druid(table_name)
    prompt = f"This is the table name {table_name} and these are its columns {columns}. Always provide only the Apache Druid SQL query to {user_input.strip()} for the table and the column provided without any additional text."

    # Initialize the Gemini model with the appropriate API
    model = genai.GenerativeModel("gemini-1.5-flash")

    # Generate content using the model
    response = model.generate_content(prompt)

    if response.candidates:
        # Extract and clean the Druid SQL query from the response
        query = response.candidates[0].content.parts[0].text.strip()
        query = query.replace("```sql\n", "").replace("```", "").strip()
        query = query.replace("```druid\n", "").replace("```", "").strip()
        query = query.replace("druid\n", "").replace("druid", "").strip()

        return {"generated_query": query}  

# Root endpoint
@app.get("/")
async def read_root():
    return {"message": "Welcome to the Druid Query Generator API",
            'queries':PREDEFINED_QUERIES}
import os
import re
import sqlite3
import json
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import requests

# --- Initialize Flask App ---
app = Flask(__name__, template_folder='templates')
CORS(app)

# --- Configuration ---
DB_FILE_PATH = 'ingres.db'
TABLE_NAME = 'state_data' # CORRECTED TABLE NAME
GEMINI_API_URL_BASE = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key='

def query_db(query, params=()):
    """Queries the database and returns a list of dictionaries."""
    try:
        conn = sqlite3.connect(DB_FILE_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return rows
    except Exception as e:
        print(f"Database query failed: {e}")
        return []

@app.route('/ask', methods=['POST'])
def ask_gemini_bot():
    data = request.json
    user_query = data.get('query', '').strip()
    api_key = data.get('api_key', '')

    if not api_key:
        return jsonify({'response': "Error: API Key is missing."}), 400

    print(f"\n[Request] User Query: \"{user_query}\"")

    # Extract keywords
    states = re.findall(r'\b(HARYANA|RAJASTHAN|PUNJAB|GUJARAT|CHHATTISGARH|JHARKHAND|TELANGANA|TAMILNADU|UTTARAKHAND|PUDUCHERRY|KARNATAKA|MAHARASHTRA)\b', user_query.upper())
    years = re.findall(r'\b(20\d{2})\b', user_query)
    states = list(set(states))
    years = list(set(years))
    
    print(f"[Debug] Found States: {states}, Found Years: {years}")

    # Build SQL query
    sql_query = f"SELECT * FROM {TABLE_NAME}"
    params = []
    conditions = []
    if states:
        conditions.append(f"State IN ({','.join(['?']*len(states))})")
        params.extend(states)
    if years:
        conditions.append(f"Year IN ({','.join(['?']*len(years))})")
        params.extend(years)
    
    if conditions:
        sql_query += " WHERE " + " AND ".join(conditions)

    retrieved_data = query_db(sql_query, tuple(params))
    print(f"[Debug] Found {len(retrieved_data)} rows from database.")

    # Construct prompt for Gemini
    system_prompt = (
        "You are an expert AI data analyst for the Indian Ground Water Resource Estimation System (INGRES). "
        "Your task is to answer the user's question based *only* on the data provided below. "
        "Be concise and clear. If the data is not available, state that clearly. "
        "When comparing states, mention both their category and their extraction percentage."
    )
    prompt = f"User Question: \"{user_query}\"\n\n--- DATA ---\n{json.dumps(retrieved_data, indent=2)}"

    try:
        payload = {"contents": [{"parts": [{"text": prompt}]}], "systemInstruction": {"parts": [{"text": system_prompt}]}}
        response = requests.post(f"{GEMINI_API_URL_BASE}{api_key}", json=payload, headers={'Content-Type': 'application/json'}, timeout=60)
        response.raise_for_status()
        
        result = response.json()
        bot_response = result['candidates'][0]['content']['parts'][0]['text']
        print(f"[Success] Gemini Response: {bot_response}")
        return jsonify({'response': bot_response})

    except Exception as e:
        print(f"--- 🔴 API or Server Error --- \n{e}")
        return jsonify({'response': f"An error occurred: {e}"}), 500

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)



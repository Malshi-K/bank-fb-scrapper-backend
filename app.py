# Flask Backend for Bank FD Rates Scraper
# Save as: app.py

from flask import Flask, jsonify, send_file, request
from flask_cors import CORS
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
from io import BytesIO
import json

app = Flask(__name__)
# CORS(app)  # Enable CORS for React frontend
CORS(app, origins=[
    "http://localhost:3000",  # Local development
    "https://bank-fd-scrapper.vercel.app/",  # Your Vercel domain
    "https://*.vercel.app"  # All Vercel apps (temporary)
])

# Bank configurations
BANK_CONFIG = {
    "NSB": {
        "url": "https://www.nsb.lk/rates-tarriffs/rupee-deposit-rates/",
        "table_index": 1,
        "special": False
    },
    "RDB": {
        "url": "https://www.rdb.lk/interest-rates/",
        "table_index": 2,
        "special": False
    },
    "SDB": {
        "url": "https://www.sdb.lk/en/rates?tableid=5",
        "table_index": 1,
        "special": False
    },
    "SMIB": {
        "url": "https://www.smib.lk/en/normal-fd-rates",
        "table_index": 0,
        "special": False
    },
    "HDFC": {
        "url": "https://www.hdfc.lk/rates-%26-tarfiffs",
        "table_index": 1,
        "special": True  # Requires BeautifulSoup extraction
    }
}

# Global storage for fetched data (in production, use Redis or a database)
fetched_data = {}

def extract_table_from_html(url, table_index=0):
    """Extract table from HTML using pandas"""
    try:
        # Add headers to mimic browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Try with pandas read_html directly first
        try:
            tables = pd.read_html(url)
        except:
            # If direct URL fails, try with requests
            response = requests.get(url, headers=headers, timeout=10)
            tables = pd.read_html(response.text)
        
        print(f"Found {len(tables)} tables at: {url}")
        
        if len(tables) > table_index:
            df = tables[table_index]
            
            # Flatten MultiIndex columns if needed
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [' '.join(col).strip() for col in df.columns.values]
            
            # Clean up the dataframe
            df = df.dropna(how='all')  # Remove completely empty rows
            df = df.reset_index(drop=True)
            
            return df
        else:
            print(f"Table index {table_index} not found on {url}")
            return None
    except Exception as e:
        print(f"Error processing {url}: {e}")
        import traceback
        traceback.print_exc()
        return None

def extract_hdfc_table(url, table_index=1):
    """Extract HDFC table using BeautifulSoup (for JS-rendered content)"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        tables = soup.find_all('table')
        print(f"HDFC: Found {len(tables)} table(s)")
        
        if table_index >= len(tables):
            print("HDFC table index out of range.")
            return None
        
        table = tables[table_index]
        
        # Extract headers
        headers = [th.get_text(strip=True) for th in table.find_all('th')]
        num_columns = len(headers)
        
        # Extract rows
        rows = []
        for tr in table.find_all('tr')[1:]:
            cells = tr.find_all('td')
            row = [cell.get_text(strip=True).replace('\n', ' ') for cell in cells]
            
            # Ensure consistent column count
            if len(row) < num_columns:
                row.extend([''] * (num_columns - len(row)))
            elif len(row) > num_columns:
                row = row[:num_columns]
                
            rows.append(row)
        
        df = pd.DataFrame(rows, columns=headers)
        df = df.dropna(how='all')  # Remove completely empty rows
        df = df.reset_index(drop=True)
        
        return df
        
    except Exception as e:
        print(f"Error extracting HDFC table: {e}")
        import traceback
        traceback.print_exc()
        return None

@app.route('/')
def home():
    """Root endpoint"""
    return jsonify({
        "message": "Bank FD Rates API (Flask)",
        "endpoints": {
            "/api/banks": "Get list of available banks",
            "/api/fetch/<bank_name>": "Fetch data for a specific bank",
            "/api/fetch-multiple": "Fetch data for multiple banks",
            "/api/export": "Export all fetched data to Excel",
            "/api/status": "Get current fetch status",
            "/api/clear": "Clear all data"
        }
    })

@app.route('/api/banks')
def get_banks():
    """Get list of available banks with their configurations"""
    return jsonify({
        "banks": list(BANK_CONFIG.keys()),
        "configurations": BANK_CONFIG
    })

@app.route('/api/fetch/<bank_name>')
def fetch_bank_data(bank_name):
    """Fetch data for a specific bank"""
    if bank_name not in BANK_CONFIG:
        return jsonify({
            "bank": bank_name,
            "status": "error",
            "message": f"Bank {bank_name} not found"
        }), 404
    
    config = BANK_CONFIG[bank_name]
    
    try:
        # Fetch data based on bank type
        if config.get("special") and bank_name == "HDFC":
            df = extract_hdfc_table(config["url"], config["table_index"])
        else:
            df = extract_table_from_html(config["url"], config["table_index"])
        
        if df is not None and not df.empty:
            # Convert DataFrame to list of dictionaries
            data = df.to_dict('records')
            
            # Store in global cache
            fetched_data[bank_name] = {
                "data": data,
                "timestamp": datetime.now().isoformat()
            }
            
            return jsonify({
                "bank": bank_name,
                "status": "success",
                "message": f"Successfully fetched {len(data)} records",
                "data": data
            })
        else:
            return jsonify({
                "bank": bank_name,
                "status": "error",
                "message": "No data found or table extraction failed"
            })
            
    except Exception as e:
        return jsonify({
            "bank": bank_name,
            "status": "error",
            "message": f"Error fetching data: {str(e)}"
        }), 500

@app.route('/api/fetch-multiple', methods=['POST'])
def fetch_multiple_banks():
    """Fetch data for multiple banks"""
    data = request.get_json()
    banks = data.get('banks', [])
    
    results = {}
    
    for bank in banks:
        if bank in BANK_CONFIG:
            result = fetch_bank_data(bank)
            results[bank] = result.get_json()
    
    return jsonify({
        "results": results,
        "summary": {
            "requested": len(banks),
            "successful": sum(1 for r in results.values() if r["status"] == "success"),
            "failed": sum(1 for r in results.values() if r["status"] == "error")
        }
    })

@app.route('/api/export')
def export_to_excel():
    """Export all fetched data to Excel file"""
    if not fetched_data:
        return jsonify({
            "status": "error",
            "message": "No data to export. Fetch some banks first."
        }), 400
    
    try:
        # Create Excel file in memory
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for bank_name, bank_data in fetched_data.items():
                if "data" in bank_data and bank_data["data"]:
                    df = pd.DataFrame(bank_data["data"])
                    # Excel sheet names max 31 chars
                    sheet_name = bank_name[:31]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        output.seek(0)
        
        # Generate filename
        filename = f"SpecialBank_FD_Rates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Error creating Excel file: {str(e)}"
        }), 500

@app.route('/api/status')
def get_status():
    """Get current status of fetched data"""
    status = {}
    
    for bank in BANK_CONFIG.keys():
        if bank in fetched_data:
            status[bank] = {
                "fetched": True,
                "timestamp": fetched_data[bank].get("timestamp"),
                "record_count": len(fetched_data[bank].get("data", []))
            }
        else:
            status[bank] = {
                "fetched": False,
                "timestamp": None,
                "record_count": 0
            }
    
    return jsonify({
        "status": status,
        "total_banks": len(BANK_CONFIG),
        "fetched_banks": sum(1 for s in status.values() if s["fetched"])
    })

@app.route('/api/clear', methods=['DELETE'])
def clear_data():
    """Clear all fetched data"""
    global fetched_data
    fetched_data = {}
    return jsonify({"message": "All fetched data cleared"})

# if __name__ == '__main__':
#     app.run(debug=True, port=5000)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
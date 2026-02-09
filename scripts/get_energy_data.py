import pandas as pd
import eurostat
import requests
import os

# --- PATH SETUP ---
# Ensures script checks/uses the correct folder for samples 
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

eu_file = os.path.join(DATA_DIR, "eu_energy_sample.csv")
russia_file = os.path.join(DATA_DIR, "russia_energy_sample.csv")

# --- CONFIGURATION ---
# Using os.getenv to use a key localy in the system
MY_API_KEY = os.getenv("EIA_API_KEY", "YOUR_API_KEY_HERE")

def get_russia_eia_data(api_key):
    """Fetches Russian monthly oil production from EIA with error handling."""
    url = f"https://api.eia.gov/v2/international/data/?api_key={api_key}"
    params = {
        "frequency": "monthly",
        "data[0]": "value",
        "facets[countryRegionId][]": "RUS",
        "facets[productId][]": "57",
        "facets[unit][]": "TBPD",
        "start": "2022-01",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc"
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        raw_data = response.json().get('response', {}).get('data', [])
        
        df = pd.DataFrame(raw_data)
        if df.empty:
            return pd.DataFrame()

        # Data Cleaning
        df = df[['period', 'value']].rename(columns={'period': 'Month', 'value': 'Production_TBPD'})
        df['Production_TBPD'] = pd.to_numeric(df['Production_TBPD'])
        df['Production_K_Tonnes'] = df['Production_TBPD'] / 7.33
        return df
    except Exception as e:
        print(f"DEBUG: EIA API unavailable ({e})")
        return pd.DataFrame()

def get_energy_data(force_update=False):
    
    # 1. EU DATA LOGIC (Eurostat)
    eu_df = pd.DataFrame()
    if not force_update and os.path.exists(eu_file):
        print("Loading EU data from local cache...")
        eu_df = pd.read_csv(eu_file)
    else:
        try:
            print("Fetching EU data from Eurostat...")
            eu_df = eurostat.get_data_df('nrg_cb_oil')
            eu_df.to_csv(eu_file, index=False)
        except Exception:
            print("Eurostat failed. Falling back to local cache.")
            if os.path.exists(eu_file):
                eu_df = pd.read_csv(eu_file)

    # 2. RUSSIA DATA LOGIC (EIA)
    russia_df = pd.DataFrame()
    # Only try API if we need an update AND we have a valid-looking key
    attempt_api = force_update or not os.path.exists(russia_file)
    has_key = MY_API_KEY and "YOUR_API_KEY" not in MY_API_KEY

    if attempt_api and has_key:
        print("Attempting to refresh Russia data from EIA...")
        russia_df = get_russia_eia_data(MY_API_KEY)
        if not russia_df.empty:
            russia_df.to_csv(russia_file, index=False)

    # FINAL FALLBACK: If API failed or was skipped, use local
    if russia_df.empty and os.path.exists(russia_file):
        print("Using local Russia data backup.")
        russia_df = pd.read_csv(russia_file)

    # Filter EU data
    eu_production = pd.DataFrame()
    if not eu_df.empty and 'nrg_bal' in eu_df.columns:
        eu_production = eu_df[eu_df['nrg_bal'].str.contains('PRD', na=False)]

    return eu_production, russia_df

# --- EXECUTION ---
eu_df, russia_df = get_energy_data()

if not russia_df.empty:
    print("\n--- Russian Production Summary ---")
    print(russia_df.head())
else:
    print("\nCRITICAL: No Russian data available (API failed and no local file found).")
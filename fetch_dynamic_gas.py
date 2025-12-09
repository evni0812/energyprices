import os
import json
import traceback
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
if os.path.exists('.env'):
    load_dotenv('.env')
elif os.path.exists('.env.development'):
    load_dotenv('.env.development')
else:
    print("No .env or .env.development file found, continuing without it")

def get_output_dir() -> str:
    """Get the appropriate output directory based on environment."""
    if os.environ.get('VERCEL'):
        return '/tmp'
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(script_dir, 'output')
        print(f"Output directory: {output_dir}")
        return output_dir

def fetch_anwb_gas_prices():
    """Fetch gas prices from ANWB API and save them to a JSON file."""
    # Set time range from 2023 till today
    start_date = datetime(2023, 12, 1)
    end_date = datetime.now()
    
    # Format dates for API URL
    start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    
    # ANWB API URL for gas
    url = f"https://api.anwb.nl/energy/energy-services/v2/tarieven/gas?startDate={start_str}&endDate={end_str}&interval=HOUR"
    print(f"Fetching gas prices from {start_date.date()} to {end_date.date()}...")
    print(f"API URL: {url}")
    
    try:
        # Fetch data from API
        print("Sending API request...")
        response = requests.get(url, timeout=60)
        print(f"API Response status code: {response.status_code}")
        
        if response.status_code != 200:
            print(f"API Error: {response.text}")
            raise ValueError(f"API returned status code {response.status_code}")
        
        # Parse JSON response
        print("Parsing JSON response...")
        data = response.json()
        
        # Debug: print data keys
        print(f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dictionary'}")
        
        if not data or not isinstance(data, dict) or 'data' not in data:
            print(f"Invalid API response: {data}")
            raise ValueError("No 'data' field found in the API response")
        
        # Process prices - filter to one price per day
        print(f"Processing {len(data['data'])} price points...")
        daily_prices = {}  # Dictionary to store one price per day
        
        for item in data['data']:
            # Parse timestamp
            timestamp = datetime.fromisoformat(item['date'].replace('Z', '+00:00'))
            
            # Extract just the date part (without time)
            date_key = timestamp.date().isoformat()
            
            # Only keep the first price we see for each day
            if date_key not in daily_prices:
                # Get allInPrijs (in cents) and convert to euros
                values = item.get('values', {})
                price_cents = values.get('allInPrijs')
                
                if price_cents is not None:
                    # Convert from cents to euros
                    price_euros = price_cents / 100.0
                    
                    # Create price entry
                    price_entry = {
                        'time': timestamp.isoformat(),
                        'price': price_euros,
                        'breakdown': {
                            'base_price': price_euros,
                            'energy_tax': 0.0,
                            'procurement_costs': 0.0
                        }
                    }
                    
                    daily_prices[date_key] = price_entry
        
        # Convert dictionary to list
        prices = list(daily_prices.values())
        
        # Sort prices by time
        prices.sort(key=lambda x: x['time'])
        
        print(f"Filtered to {len(prices)} daily price points (one per day)...")
        
        # Create output data structure
        output_data = {
            'last_updated': datetime.now().isoformat(),
            'prices': prices
        }
        
        # Save to output directory
        output_dir = get_output_dir()
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, 'anwb_gas_prices.json')
        
        print(f"Saving {len(prices)} price points to {output_path}...")
        
        # Save as JSON
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        # If we're on Vercel, also save to public directory
        if os.environ.get('VERCEL'):
            public_dir = os.path.join(os.getcwd(), 'public', 'data')
            os.makedirs(public_dir, exist_ok=True)
            public_path = os.path.join(public_dir, 'anwb_gas_prices.json')
            with open(public_path, 'w') as f:
                json.dump(output_data, f, indent=2)
        
        print(f"ANWB gas prices saved to {output_path}")
        print(f"Total number of price points: {len(prices)}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching ANWB data: {e}")
        traceback.print_exc()
    except ValueError as e:
        print(f"Error processing data: {e}")
        traceback.print_exc()
    except Exception as e:
        print(f"Unexpected error: {e}")
        traceback.print_exc()

def fetch_anwb_gas_prices_batch(start_date, end_date):
    """
    Fetch gas prices from the ANWB API for a single batch.
    Returns a dict of {date_key: {time, price}} for daily prices.
    """
    import time as time_module
    start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    
    url = f"https://api.anwb.nl/energy/energy-services/v2/tarieven/gas?startDate={start_str}&endDate={end_str}&interval=HOUR"
    response = requests.get(url, timeout=60)
    if response.status_code != 200:
        raise ValueError(f"API returned status code {response.status_code}: {response.text}")
    data = response.json()
    if not data or not isinstance(data, dict) or 'data' not in data:
        raise ValueError("No 'data' field found in the API response")
    
    daily_prices = {}
    for item in data['data']:
        timestamp = datetime.fromisoformat(item['date'].replace('Z', '+00:00'))
        date_key = timestamp.date().isoformat()
        if date_key not in daily_prices:
            values = item.get('values', {})
            price_cents = values.get('allInPrijs')
            if price_cents is not None:
                daily_prices[date_key] = {
                    'time': timestamp,
                    'price': price_cents / 100.0
                }
    return daily_prices


def get_dynamic_gas_prices(start_date=None):
    """
    Haal de all-in gasprijzen (incl. BTW) per dag op vanaf start_date (default: 1 dec 2023).
    Geeft een pandas DataFrame terug met kolommen: time (UTC, datetime), price (EUR/m3).
    Fetches in batches of 90 days to avoid API timeouts.
    """
    import time as time_module
    
    if start_date is None:
        start_date = datetime(2023, 12, 1)
    end_date = datetime.now()
    
    print(f"Fetching gas prices from ANWB API for period: {start_date.date()} to {end_date.date()}")
    
    # Fetch in batches of 90 days
    batch_days = 90
    all_daily_prices = {}
    current_start = start_date
    batch_num = 0
    
    while current_start < end_date:
        batch_num += 1
        current_end = min(current_start + timedelta(days=batch_days), end_date)
        
        print(f"  Batch {batch_num}: {current_start.date()} to {current_end.date()}...")
        
        try:
            daily_prices = fetch_anwb_gas_prices_batch(current_start, current_end)
            all_daily_prices.update(daily_prices)
            print(f"    -> {len(daily_prices)} dagen opgehaald")
        except Exception as e:
            print(f"    -> Error in batch: {str(e)}")
        
        current_start = current_end
        time_module.sleep(0.5)  # Rate limiting
    
    df = pd.DataFrame(list(all_daily_prices.values()))
    if len(df) > 0:
        if not pd.api.types.is_datetime64_dtype(df['time']):
            df['time'] = pd.to_datetime(df['time'], utc=True)
        if df['time'].dt.tz is not None and str(df['time'].dt.tz) != 'UTC':
            df['time'] = df['time'].dt.tz_convert('UTC')
        df = df.sort_values('time').reset_index(drop=True)
    
    print(f"Successfully processed {len(df)} total daily prices")
    return df

if __name__ == '__main__':
    print("Script started")
    fetch_anwb_gas_prices()
    print("Script finished")
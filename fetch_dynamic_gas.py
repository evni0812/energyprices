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

def get_energy_tax_rate(timestamp: datetime) -> float:
    """
    Return the appropriate energy tax rate for gas based on the year.
    
    Args:
        timestamp: The datetime for which to get the tax rate
        
    Returns:
        float: The energy tax rate for that year (including VAT)
    """
    year = timestamp.year
    
    if year == 2024:
        return 0.70544
    elif year == 2025:
        return 0.69957
    else:
        # Default to the latest known rate if we don't have data for this year
        return 0.69957

def fetch_ez_gas_prices():
    """Fetch gas prices from EnergyZero API and save them to a JSON file."""
    # Set time range from 2023 till today
    start_date = datetime(2023, 12, 1)
    end_date = datetime.now()
    
    # Format dates for API URL
    from_date = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    till_date = end_date.strftime("%Y-%m-%dT%H:%M:%S.999Z")
    
    # EnergyZero API URL
    url = f"https://api.energyzero.nl/v1/energyprices?fromDate={from_date}&tillDate={till_date}&interval=5&usageType=3&inclBtw=true"
    print(f"Fetching gas prices from {start_date.date()} to {end_date.date()}...")
    print(f"API URL: {url}")
    
    try:
        # Fetch data from API
        print("Sending API request...")
        response = requests.get(url, timeout=30)
        print(f"API Response status code: {response.status_code}")
        
        if response.status_code != 200:
            print(f"API Error: {response.text}")
            raise ValueError(f"API returned status code {response.status_code}")
        
        # Parse JSON response
        print("Parsing JSON response...")
        data = response.json()
        
        # Debug: print data keys
        print(f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dictionary'}")
        
        if not data or not isinstance(data, dict) or 'Prices' not in data:
            print(f"Invalid API response: {data}")
            raise ValueError("No prices found in the API response")
        
        # Process prices - filter to one price per day
        print(f"Processing {len(data['Prices'])} price points...")
        daily_prices = {}  # Dictionary to store one price per day
        
        for item in data['Prices']:
            # Parse timestamp
            timestamp = datetime.fromisoformat(item['readingDate'].replace('Z', '+00:00'))
            
            # Extract just the date part (without time)
            date_key = timestamp.date().isoformat()
            
            # Only keep the first price we see for each day
            if date_key not in daily_prices:
                # Get original price (already includes VAT)
                original_price = item['price']
                
                # Get energy tax rate based on year (already includes VAT)
                energy_tax = get_energy_tax_rate(timestamp)
                
                # Procurement costs (already includes VAT)
                procurement_costs = 0.05911
                
                # Calculate total price
                total_price = original_price + energy_tax + procurement_costs
                
                # Create price entry
                price_entry = {
                    'time': timestamp.isoformat(),
                    'price': total_price,
                    'breakdown': {
                        'base_price': original_price,
                        'energy_tax': energy_tax,
                        'procurement_costs': procurement_costs
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
        output_path = os.path.join(output_dir, 'ez_gas_prices.json')
        
        print(f"Saving {len(prices)} price points to {output_path}...")
        
        # Save as JSON
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        # If we're on Vercel, also save to public directory
        if os.environ.get('VERCEL'):
            public_dir = os.path.join(os.getcwd(), 'public', 'data')
            os.makedirs(public_dir, exist_ok=True)
            public_path = os.path.join(public_dir, 'ez_gas_prices.json')
            with open(public_path, 'w') as f:
                json.dump(output_data, f, indent=2)
        
        print(f"Energy Zero gas prices saved to {output_path}")
        print(f"Total number of price points: {len(prices)}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Energy Zero data: {e}")
        traceback.print_exc()
    except ValueError as e:
        print(f"Error processing data: {e}")
        traceback.print_exc()
    except Exception as e:
        print(f"Unexpected error: {e}")
        traceback.print_exc()

def get_dynamic_gas_prices(start_date=None):
    """
    Haal de kale gasprijzen (zonder belasting, zonder procurement_costs) per dag op vanaf start_date (default: 1 dec 2023).
    Geeft een pandas DataFrame terug met kolommen: time (UTC, datetime), price (EUR/m3).
    """
    if start_date is None:
        start_date = datetime(2023, 12, 1)
    end_date = datetime.now()
    from_date = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    till_date = end_date.strftime("%Y-%m-%dT%H:%M:%S.999Z")
    url = f"https://api.energyzero.nl/v1/energyprices?fromDate={from_date}&tillDate={till_date}&interval=5&usageType=3&inclBtw=true"
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        raise ValueError(f"API returned status code {response.status_code}: {response.text}")
    data = response.json()
    if not data or not isinstance(data, dict) or 'Prices' not in data:
        raise ValueError("No prices found in the API response")
    daily_prices = {}
    for item in data['Prices']:
        timestamp = datetime.fromisoformat(item['readingDate'].replace('Z', '+00:00'))
        date_key = timestamp.date().isoformat()
        if date_key not in daily_prices:
            original_price = item['price']
            daily_prices[date_key] = {
                'time': timestamp,
                'price': original_price
            }
    df = pd.DataFrame(list(daily_prices.values()))
    if len(df) > 0:
        if not pd.api.types.is_datetime64_dtype(df['time']):
            df['time'] = pd.to_datetime(df['time'], utc=True)
        if df['time'].dt.tz is not None and str(df['time'].dt.tz) != 'UTC':
            df['time'] = df['time'].dt.tz_convert('UTC')
        df = df.sort_values('time').reset_index(drop=True)
    return df

if __name__ == '__main__':
    print("Script started")
    fetch_ez_gas_prices()
    print("Script finished")
import os
import json
import time
import pytz
import requests
import traceback
from datetime import datetime, timedelta
import pandas as pd
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# Bepaal de juiste output directory
def get_output_dir() -> str:
    """Get the appropriate output directory based on environment."""
    if os.environ.get('VERCEL'):
        return '/tmp'
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(script_dir, 'output')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        return output_dir

def calculate_energy_tax_rate(timestamp):
    """Calculate energy tax rate based on timestamp."""
    year = timestamp.year
    
    # Energy tax rates per year (simplified version)
    tax_rates = {
        2023: 0.12599,
        2024: 0.10880,
        2025: 0.10154,  
        2026: 0.10154, 
    }
    
    # Use most recent year if the year is not in the dictionary
    if year not in tax_rates:
        year = max(tax_rates.keys())
    
    return tax_rates[year]

def fetch_energyzero_prices(start_date, end_date):
    """
    Fetch energy prices from the EnergyZero API.
    
    Args:
        start_date (datetime): The start date for the price data.
        end_date (datetime): The end date for the price data.
        
    Returns:
        pandas.DataFrame: DataFrame with price data.
    """
    print(f"Fetching energy prices from EnergyZero API for period: {start_date} to {end_date}")
    
    # Format dates for the API - using standard URL encoding
    from_date = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    till_date = end_date.strftime("%Y-%m-%dT%H:%M:%S.999Z")
    
    # EnergyZero API URL with parameters
    url = f"https://api.energyzero.nl/v1/energyprices?fromDate={from_date}&tillDate={till_date}&interval=4&usageType=1&inclBtw=true"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Print the raw response for debugging
        print(f"API response status: {response.status_code}")
        data = response.json()
        
        # Debug: Print out the structure of the API response
        print(f"API response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dictionary'}")
        
        # Attempt to extract price data based on different possible structures
        prices = []
        
        if isinstance(data, dict):
            # Try multiple possible paths to extract price data
            if 'data' in data and 'Prices' in data['data']:
                prices = data['data']['Prices']
                print(f"Found {len(prices)} price points in data.Prices")
            elif 'Prices' in data:
                prices = data['Prices']
                print(f"Found {len(prices)} price points in Prices")
            elif 'prices' in data:
                prices = data['prices']
                print(f"Found {len(prices)} price points in prices")
            elif 'result' in data and isinstance(data['result'], list):
                prices = data['result']
                print(f"Found {len(prices)} price points in result")
            else:
                # If we can't find a clear prices array, check all arrays in the response
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 0:
                        if isinstance(value[0], dict) and ('price' in value[0] or 'readingDate' in value[0]):
                            prices = value
                            print(f"Found {len(prices)} price points in {key}")
                            break
        
        if not prices and response.text:
            # If we still don't have prices but got content, try parsing possible array in the response
            try:
                content = response.text
                if content.startswith('[') and content.endswith(']'):
                    prices = json.loads(content)
                    print(f"Parsed array from response text, found {len(prices)} entries")
            except json.JSONDecodeError:
                pass
        
        if not prices:
            raise ValueError("API response does not contain price data in expected format")
            
        # Convert the data to DataFrame
        all_prices = []
        for item in prices:
            try:
                timestamp = None
                price = None
                
                # Try different field names for timestamp
                for field in ['readingDate', 'timestamp', 'datetime', 'date', 'time']:
                    if field in item:
                        timestamp = item[field]
                        break
                
                # Try different field names for price
                for field in ['price', 'Price', 'value', 'Value']:
                    if field in item:
                        price = item[field]
                        break
                
                if timestamp and price is not None:  # Allow price to be 0
                    all_prices.append({
                        'time': timestamp,
                        'price': price
                    })
            except Exception as e:
                print(f"Error processing price point: {str(e)}")
                continue
                
        print(f"Successfully processed {len(all_prices)} price points")
        return pd.DataFrame(all_prices)
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {str(e)}")
        return None
    except ValueError as e:
        print(f"Error fetching data from API: {str(e)}")
        return None
    except Exception as e:
        print(f"Unexpected error fetching data: {str(e)}")
        traceback.print_exc()
        return None

def analyze_data_completeness(prices_df):
    """
    Analyze the completeness of the price data without filling gaps.
    
    Args:
        prices_df (pandas.DataFrame): DataFrame with price data.
        
    Returns:
        pandas.DataFrame: The original DataFrame.
    """
    if prices_df is None or len(prices_df) == 0:
        print("No data to analyze")
        return prices_df
    
    print("Analyzing data completeness...")
    
    # Ensure time column is datetime with UTC timezone
    if not pd.api.types.is_datetime64_dtype(prices_df['time']):
        prices_df['time'] = pd.to_datetime(prices_df['time'], utc=True)
    
    # Make sure all times are in UTC
    if prices_df['time'].dt.tz is not None and str(prices_df['time'].dt.tz) != 'UTC':
        prices_df['time'] = prices_df['time'].dt.tz_convert('UTC')
    
    # Get the start and end times
    start_time = prices_df['time'].min()
    end_time = prices_df['time'].max()
    
    # Create a complete range of hourly timestamps in UTC
    complete_range = pd.date_range(start=start_time, end=end_time, freq='h', tz='UTC')
    complete_df = pd.DataFrame({'time': complete_range})
    
    # Merge to find missing hours
    merged_df = pd.merge(complete_df, prices_df, on='time', how='left')
    missing_hours = merged_df[merged_df['price'].isna()]
    
    if len(missing_hours) > 0:
        print(f"Found {len(missing_hours)} missing hours in the dataset")
        
        # Analyze missing hours by month
        missing_hours['month'] = missing_hours['time'].dt.month
        missing_hours['year'] = missing_hours['time'].dt.year
        missing_by_month = missing_hours.groupby(['year', 'month']).size().reset_index(name='count')
        
        print("\nMissing hours by month:")
        for _, row in missing_by_month.iterrows():
            print(f"  {row['year']}-{row['month']:02d}: {row['count']} hours missing")
        
        # Check for daylight saving time transitions
        amsterdam = pytz.timezone('Europe/Amsterdam')
        dst_transitions = []
        
        for year in range(start_time.year, end_time.year + 1):
            # Last Sunday of March (DST starts)
            march_last_sunday = datetime(year, 3, 31, 2, 0, tzinfo=amsterdam)
            while march_last_sunday.weekday() != 6:  # 6 is Sunday
                march_last_sunday -= timedelta(days=1)
            
            # Last Sunday of October (DST ends)
            october_last_sunday = datetime(year, 10, 31, 3, 0, tzinfo=amsterdam)
            while october_last_sunday.weekday() != 6:  # 6 is Sunday
                october_last_sunday -= timedelta(days=1)
            
            dst_transitions.extend([march_last_sunday, october_last_sunday])
        
        print("\nDST transition dates (these may cause expected 'missing' hours):")
        for transition in dst_transitions:
            print(f"  {transition.strftime('%Y-%m-%d %H:%M')} {'(DST starts)' if transition.month == 3 else '(DST ends)'}")
        
        print("\nFirst 10 missing timestamps (if any):")
        for idx, row in missing_hours.head(10).iterrows():
            print(f"  {row['time'].strftime('%Y-%m-%d %H:%M')} UTC")
    else:
        print("No missing hours found in the dataset")
    
    # Check for duplicate timestamps
    duplicate_times = prices_df[prices_df.duplicated('time', keep=False)]
    if len(duplicate_times) > 0:
        print(f"\nFound {len(duplicate_times)} duplicate timestamps")
        print("First 5 duplicates:")
        for idx, row in duplicate_times.head(5).iterrows():
            print(f"  {row['time'].strftime('%Y-%m-%d %H:%M')}: {row['price']}")
    else:
        print("No duplicate timestamps found")
    
    return prices_df

def fetch_entsoe_prices():
    """Fetch energy prices from the API and save them."""
    # Define date range (past 450 days + 1 day into future)
    end_date = datetime.now() + timedelta(days=1)
    start_date = end_date - timedelta(days=450)
    
    print(f"Fetching energy prices for period: {start_date} to {end_date}")
    
    # Fetch prices from EnergyZero API
    all_prices = fetch_energyzero_prices(start_date, end_date)
    
    # Check if we have valid data
    if all_prices is None or len(all_prices) == 0:
        print("Failed to fetch price data. No data will be saved.")
        return
    
    # Convert timestamps to datetime objects if they're not already
    print("Converting timestamps to Amsterdam timezone...")
    if not pd.api.types.is_datetime64_dtype(all_prices['time']):
        all_prices['time'] = pd.to_datetime(all_prices['time'], utc=True)
    
    # Convert timezone to Amsterdam time
    amsterdam = pytz.timezone('Europe/Amsterdam')
    all_prices['time'] = all_prices['time'].dt.tz_convert(amsterdam)
    
    # Analyze data completeness (without filling gaps)
    all_prices = analyze_data_completeness(all_prices)
    
    # Calculate the price breakdown including energy tax and procurement costs
    print("Calculating price breakdown...")
    all_prices['base_price'] = all_prices['price']
    
    # Add energy tax (estimated)
    all_prices['energy_tax'] = all_prices['time'].apply(calculate_energy_tax_rate)
    
    # Add procurement costs
    all_prices['procurement_costs'] = 0.04  # Simplified procurement costs

    # Calculate VAT (21% on all components)
    all_prices['vat'] = (all_prices['base_price'] + all_prices['energy_tax'] + all_prices['procurement_costs']) * 0.21
    
    # Calculate total price (including VAT)
    all_prices['total_price'] = all_prices['base_price'] + all_prices['energy_tax'] + all_prices['procurement_costs'] + all_prices['vat']
    
    # Convert datetime objects to strings to make them JSON serializable
    all_prices['time'] = all_prices['time'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    
    # Save the data to JSON
    output_dir = get_output_dir()
    output_file = os.path.join(output_dir, 'energy_prices.json')
    
    # Additional copy for reference
    extra_file = os.path.join(output_dir, 'energyzero_data.json')
    
    # Convert to JSON
    json_data = all_prices.to_dict(orient='records')
    
    # Herstructureer de data naar het expected format voor entsoe_data.json
    formatted_data = {
        'last_updated': datetime.now().isoformat(),
        'prices': []
    }
    
    # Zet elke record in het juiste format met time, price en breakdown structuur
    for item in json_data:
        formatted_price = {
            'time': item['time'],
            'price': item['total_price'],  # Gebruik total_price als de hoofdprijs
            'breakdown': {
                'base_price': item['base_price'],  # Basis prijs zonder belastingen
                'energy_tax': item.get('energy_tax', 0),  # Energiebelasting
                'procurement_costs': item.get('procurement_costs', 0),  # Inkoopkosten
                'vat': item.get('vat', 0)  # BTW (21%)
            }
        }
        formatted_data['prices'].append(formatted_price)
    
    # Save to the json file in the proper format voor combine_data.py
    with open(output_file, 'w') as f:
        json.dump(formatted_data, f, indent=2)
    
    # Save an extra copy of de original data for reference
    with open(extra_file, 'w') as f:
        json.dump({
            'last_updated': datetime.now().isoformat(),
            'data': json_data
        }, f, indent=2)
    
    print(f"Saved {len(json_data)} price points to {output_file} in entsoe_data.json compatible format")
    print(f"Original data saved to {extra_file} for reference")

    # Als we op Vercel draaien, maak ook een kopie voor entsoe_data.json
    if os.environ.get('VERCEL'):
        entsoe_file = os.path.join(output_dir, 'entsoe_data.json')
        with open(entsoe_file, 'w') as f:
            json.dump(formatted_data, f, indent=2)
        print(f"Also created entsoe_data.json copy for Vercel deployment")

def get_dynamic_electricity_prices(start_date=None):
    """
    Haal de kale elektriciteitsprijzen (excl. belasting, excl. procurement_costs) per uur op vanaf start_date (default: 450 dagen terug).
    Geeft een pandas DataFrame terug met kolommen: time (UTC), price (EUR/kWh).
    """
    if start_date is None:
        end_date = datetime.now() + timedelta(days=1)
        start_date = end_date - timedelta(days=450)
    else:
        end_date = datetime.now() + timedelta(days=1)
    df = fetch_energyzero_prices(start_date, end_date)
    if df is not None and len(df) > 0:
        # Zorg dat tijd in UTC staat en als datetime
        if not pd.api.types.is_datetime64_dtype(df['time']):
            df['time'] = pd.to_datetime(df['time'], utc=True)
        if df['time'].dt.tz is not None and str(df['time'].dt.tz) != 'UTC':
            df['time'] = df['time'].dt.tz_convert('UTC')
        # Alleen kolommen time en price
        return df[['time', 'price']].copy()
    return df

if __name__ == "__main__":
    fetch_entsoe_prices()  # Keep the same function name for compatibility 
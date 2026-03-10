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
    """
    Haal gasprijzen op uit de maandgrafiek (ANWB MONTH) en sla all-in tarief op.
    Zelfde aanpak als elektriciteit: alleen maanddata, all-in, geen extra aanpassingen.
    """
    start_date = datetime(2023, 12, 1)
    df = get_dynamic_gas_prices(start_date=start_date, interval='MONTH')
    if df is None or len(df) == 0:
        print("Failed to fetch gas price data. No data will be saved.")
        return
    try:
        output_dir = get_output_dir()
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, 'anwb_gas_prices.json')
        # Zelfde formaat als voorheen: time, price (all-in), breakdown (base_price=price, rest 0)
        prices = []
        for _, row in df.iterrows():
            t = row['time']
            ts_str = t.isoformat() if hasattr(t, 'isoformat') else str(t)
            price = float(row['price'])
            prices.append({
                'time': ts_str,
                'price': round(price, 5),
                'breakdown': {
                    'base_price': round(price, 5),
                    'energy_tax': 0.0,
                    'procurement_costs': 0.0
                }
            })
        output_data = {
            'last_updated': datetime.now().isoformat(),
            'prices': prices
        }
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2)
        if os.environ.get('VERCEL'):
            public_dir = os.path.join(os.getcwd(), 'public', 'data')
            os.makedirs(public_dir, exist_ok=True)
            with open(os.path.join(public_dir, 'anwb_gas_prices.json'), 'w') as f:
                json.dump(output_data, f, indent=2)
        print(f"ANWB gas prices saved to {output_path} ({len(prices)} monthly points)")
    except Exception as e:
        print(f"Error saving gas data: {e}")
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


def fetch_anwb_gas_prices_monthly_interval(start_date, end_date):
    """
    Haal maandgemiddelden op via het MONTH interval van de ANWB gas API (maandgrafiek).

    Response date is eind-van-maand UTC = start volgende maand in Amsterdam.
    Voorbeeld: 2025-12-31T23:00:00Z = 2026-01-01 00:00 CET = data voor januari 2026.
    Gebruik allInPrijs (cent) → EUR; geen extra aanpassingen.

    Geeft een DataFrame terug met kolommen: time (UTC, eerste van de maand), price (EUR/m3).
    """
    from zoneinfo import ZoneInfo
    from datetime import timezone

    amsterdam = ZoneInfo('Europe/Amsterdam')
    start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    url = (f"https://api.anwb.nl/energy/energy-services/v2/tarieven/gas"
           f"?startDate={start_str}&endDate={end_str}&interval=MONTH")

    response = requests.get(url, timeout=60)
    if response.status_code != 200:
        raise ValueError(f"API returned {response.status_code}: {response.text}")
    data = response.json()

    prices = []
    for item in data.get('data', []):
        ts_utc = datetime.fromisoformat(item['date'].replace('Z', '+00:00'))
        ts_ams = ts_utc.astimezone(amsterdam)
        month_start_utc = datetime(ts_ams.year, ts_ams.month, 1, tzinfo=timezone.utc)
        price_cents = item.get('values', {}).get('allInPrijs')
        if price_cents is not None:
            prices.append({'time': month_start_utc, 'price': round(price_cents / 100.0, 5)})

    if not prices:
        return None
    df = pd.DataFrame(prices)
    df['time'] = pd.to_datetime(df['time'], utc=True)
    return df


def get_dynamic_gas_prices(start_date=None, interval='MONTH'):
    """
    Haal de all-in gasprijzen (incl. BTW) op vanaf start_date.

    interval: 'MONTH' (default) of 'HOUR'.
    - MONTH: alleen maanddata uit de maandgrafiek (ANWB MONTH), all-in tarief.
    - HOUR: probeert uurtarieven in batches en vult aan met MONTH waar ontbreekt.

    Geeft een pandas DataFrame terug met kolommen: time (UTC), price (EUR/m3).
    """
    if start_date is None:
        start_date = datetime(2023, 12, 1)
    end_date = datetime.now() + timedelta(days=1)

    # MONTH als default: alleen data uit maandgrafiek, all-in tarief
    if interval == 'MONTH':
        try:
            # Trek 1 dag af van start_date zodat de startmaand niet gemist wordt (zelfde als elektra)
            df_month = fetch_anwb_gas_prices_monthly_interval(
                start_date - timedelta(days=1), end_date
            )
        except Exception as e:
            print(f"[WARN] Gas MONTH interval mislukt: {e}")
            return None
        if df_month is None or len(df_month) == 0:
            return None
        print(f"Successfully processed {len(df_month)} monthly gas prices (MONTH interval)")
        return df_month[['time', 'price']].copy()

    # HOUR: batches + MONTH als aanvulling
    import time as time_module
    print(f"Fetching gas prices from ANWB API for period: {start_date.date()} to {end_date.date()}")

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
        time_module.sleep(0.5)

    df_daily = pd.DataFrame(list(all_daily_prices.values())) if all_daily_prices else pd.DataFrame()
    if len(df_daily) > 0:
        if not pd.api.types.is_datetime64_dtype(df_daily['time']):
            df_daily['time'] = pd.to_datetime(df_daily['time'], utc=True)
        if df_daily['time'].dt.tz is not None and str(df_daily['time'].dt.tz) != 'UTC':
            df_daily['time'] = df_daily['time'].dt.tz_convert('UTC')
        df_daily = df_daily.sort_values('time').reset_index(drop=True)

    try:
        df_month = fetch_anwb_gas_prices_monthly_interval(start_date, end_date)
    except Exception as e:
        print(f"[WARN] Gas MONTH interval mislukt: {e}")
        df_month = None

    if df_month is None or len(df_month) == 0:
        return df_daily[['time', 'price']].copy() if len(df_daily) > 0 else None

    if len(df_daily) == 0:
        return df_month[['time', 'price']].copy()

    daily_months = set(df_daily['time'].dt.strftime('%Y-%m'))
    df_month_extra = df_month[~df_month['time'].dt.strftime('%Y-%m').isin(daily_months)]
    combined = (pd.concat([df_daily, df_month_extra], ignore_index=True)
                .sort_values('time').reset_index(drop=True))
    print(f"Successfully processed {len(combined)} total prices (daily + MONTH supplement)")
    return combined[['time', 'price']].copy()

if __name__ == '__main__':
    print("Script started")
    fetch_anwb_gas_prices()
    print("Script finished")
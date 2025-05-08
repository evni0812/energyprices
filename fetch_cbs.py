import os
import json
from datetime import datetime
import requests

def get_output_dir() -> str:
    """Get the appropriate output directory based on environment."""
    if os.environ.get('VERCEL'):
        return '/tmp'
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(script_dir, 'output')

def get_cbs_rates():
    """
    Haal de CBS elektriciteits- en gasprijzen (inclusief btw) per maand op.
    Geeft een lijst van dicts terug met per maand de tarieven en energiebelasting.
    """
    url = "https://opendata.cbs.nl/ODataApi/odata/85592NED/TypedDataSet"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    rates = []
    for item in data.get('value', []):
        if (item.get('Btw') == 'A048944' and 
            item.get('Perioden') and 
            'MM' in item.get('Perioden', '')):
            try:
                period = item['Perioden']
                year = int(period[:4])
                month = int(period[-2:])
                rate_entry = {
                    'period': f"{year}-{month:02d}"
                }
                has_electricity_data = (item.get('VariabelLeveringstariefContractprijs_9') is not None and
                                       item.get('Energiebelasting_12') is not None)
                if has_electricity_data:
                    electricity_base_rate = float(item['VariabelLeveringstariefContractprijs_9'])
                    electricity_energy_tax = float(item['Energiebelasting_12'])
                    electricity_total = electricity_base_rate + electricity_energy_tax
                    rate_entry.update({
                        'base_rate': electricity_base_rate,
                        'energy_tax': electricity_energy_tax,
                        'total': electricity_total,
                    })
                has_gas_data = (item.get('VariabelLeveringstariefContractprijs_3') is not None and
                                item.get('Energiebelasting_6') is not None)
                if has_gas_data:
                    gas_base_rate = float(item['VariabelLeveringstariefContractprijs_3'])
                    gas_energy_tax = float(item['Energiebelasting_6'])
                    gas_total = gas_base_rate + gas_energy_tax
                    rate_entry.update({
                        'gas_base_rate': gas_base_rate,
                        'gas_energy_tax': gas_energy_tax,
                        'gas_total': gas_total,
                    })
                if has_electricity_data or has_gas_data:
                    rates.append(rate_entry)
            except (ValueError, TypeError):
                continue
    rates.sort(key=lambda x: x['period'])
    return rates

def fetch_cbs_rates():
    try:
        print("Fetching CBS electricity and gas prices data...")
        rates = get_cbs_rates()
        output_data = {
            'last_updated': datetime.now().isoformat(),
            'rates': rates
        }
        output_dir = get_output_dir()
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, 'cbs_rates.json')
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2)
        if os.environ.get('VERCEL'):
            public_dir = os.path.join(os.getcwd(), 'public', 'data')
            os.makedirs(public_dir, exist_ok=True)
            public_path = os.path.join(public_dir, 'cbs_rates.json')
            with open(public_path, 'w') as f:
                json.dump(output_data, f, indent=2)
        print(f"CBS rates saved to {output_path}")
        print(f"Total number of rates: {len(rates)}")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching CBS data: {e}")
        raise
    except ValueError as e:
        print(f"Error processing data: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise

if __name__ == '__main__':
    fetch_cbs_rates() 
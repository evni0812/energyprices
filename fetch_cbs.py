import os
import json
import ssl
import time
import urllib3
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class TLS12Adapter(HTTPAdapter):
    """
    HTTPS adapter dat TLS 1.2 afdwingt.
    Nodig omdat opendata.cbs.nl de verbinding verbreekt met Python 3.12+ op macOS.
    Op Linux/GitHub Actions werkt dit transparant; er is geen nadeel aan TLS 1.2.
    """
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2
        kwargs['ssl_context'] = ctx
        super().init_poolmanager(*args, **kwargs)


def get_output_dir() -> str:
    """Get the appropriate output directory based on environment."""
    if os.environ.get('VERCEL'):
        return '/tmp'
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(script_dir, 'output')


def _make_cbs_session() -> requests.Session:
    session = requests.Session()
    session.mount("https://opendata.cbs.nl", TLS12Adapter())
    return session


def get_cbs_rates(max_retries=4, retry_delay=15):
    """
    Haal de CBS elektriciteits- en gasprijzen (inclusief btw) per maand op.
    Geeft een lijst van dicts terug met per maand de tarieven en energiebelasting.

    Veldmapping in dataset 85592NED (bijgewerkt feb 2026 – CBS voegde dynamisch-tarief
    velden _11 en _12 toe, waardoor Energiebelasting elektriciteit verschoof van _12 → _14):
      Gas:       VariabelLeveringstariefContractprijs_3 + Energiebelasting_6
      Elektra:   VariabelLeveringstariefContractprijs_9 + Energiebelasting_14

    Bij tijdelijke netwerkstoringen (DNS, timeout) wordt max_retries keer opnieuw
    geprobeerd met retry_delay seconden pauze.
    """
    url = "https://opendata.cbs.nl/ODataApi/odata/85592NED/TypedDataSet"
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            session = _make_cbs_session()
            response = session.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            break  # succes, ga verder met verwerking
        except Exception as e:
            last_error = e
            if attempt < max_retries:
                print(f"[WARN] CBS poging {attempt}/{max_retries} mislukt "
                      f"({type(e).__name__}), retry in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                raise last_error
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
                # Elektriciteit: veld _14 is Energiebelasting na CBS-herstructurering feb 2026
                # (was _12 vóór die datum)
                has_electricity_data = (item.get('VariabelLeveringstariefContractprijs_9') is not None and
                                        item.get('Energiebelasting_14') is not None)
                if has_electricity_data:
                    electricity_base_rate = float(item['VariabelLeveringstariefContractprijs_9'])
                    electricity_energy_tax = float(item['Energiebelasting_14'])
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
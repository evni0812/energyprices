import os
import pandas as pd
from fetch_cbs import get_cbs_rates
from fetch_dynamic_electricity import get_dynamic_electricity_prices
from fetch_dynamic_gas import get_dynamic_gas_prices
from datetime import datetime
import hashlib

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
PROCUREMENT_COSTS_ELEC = 0.04840
PROCUREMENT_COSTS_GAS = 0.05911

# Originele data elektriciteit (jan-21 t/m mar-25)
ORIG_ELEC = [
    ["jan-21", 0.22, 0.22], ["feb-21", 0.22, 0.22], ["mar-21", 0.23, 0.23], ["apr-21", 0.23, 0.23],
    ["may-21", 0.23, 0.23], ["jun-21", 0.23, 0.26], ["jul-21", 0.24, 0.27], ["aug-21", 0.24, 0.27],
    ["sep-21", 0.26, 0.33], ["oct-21", 0.28, 0.36], ["nov-21", 0.33, 0.39], ["dec-21", 0.37, 0.45],
    ["jan-22", 0.4, 0.33], ["feb-22", 0.38, 0.3], ["mar-22", 0.56, 0.41], ["apr-22", 0.52, 0.33],
    ["may-22", 0.44, 0.32], ["jun-22", 0.42, 0.35], ["jul-22", 0.49, 0.41], ["aug-22", 0.58, 0.54],
    ["sep-22", 0.72, 0.43], ["oct-22", 0.73, 0.25], ["nov-22", 0.6, 0.29], ["dec-22", 0.59, 0.37],
    ["jan-23", 0.67, 0.32], ["feb-23", 0.63, 0.33], ["mar-23", 0.58, 0.3], ["apr-23", 0.45, 0.29],
    ["may-23", 0.4, 0.26], ["jun-23", 0.39, 0.28], ["jul-23", 0.35, 0.26], ["aug-23", 0.35, 0.28],
    ["sep-23", 0.35, 0.29], ["oct-23", 0.35, 0.28], ["nov-23", 0.35, 0.29], ["dec-23", 0.35, 0.26],
    ["jan-24", 0.33, 0.3], ["feb-24", 0.33, 0.25], ["mar-24", 0.33, 0.26], ["apr-24", 0.31, 0.25],
    ["may-24", 0.3, 0.27], ["jun-24", 0.3, 0.26], ["jul-24", 0.29, 0.26], ["aug-24", 0.28, 0.27],
    ["sep-24", 0.28, 0.27], ["oct-24", 0.29, 0.29], ["nov-24", 0.29, 0.32], ["dec-24", 0.29, 0.31],
    ["jan-25", 0.29, 0.31], ["feb-25", 0.28, 0.32], ["mar-25", 0.28, 0.28]
]
# Originele data gas (jan-21 t/m mar-25)
ORIG_GAS = [
    ["jan-21", 0.8, 0.82], ["feb-21", 0.8, 0.8], ["mar-21", 0.81, 0.79], ["apr-21", 0.81, 0.82],
    ["may-21", 0.81, 0.88], ["jun-21", 0.83, 0.92], ["jul-21", 0.87, 1.01], ["aug-21", 0.88, 1.1],
    ["sep-21", 0.93, 1.31], ["oct-21", 1.05, 1.64], ["nov-21", 1.27, 1.53], ["dec-21", 1.42, 1.92],
    ["jan-22", 1.74, 1.6], ["feb-22", 1.67, 1.56], ["mar-22", 2.39, 2.15], ["apr-22", 2.23, 1.81],
    ["may-22", 1.97, 1.66], ["jun-22", 1.76, 1.82], ["jul-22", 2.08, 2.19], ["aug-22", 2.6, 2.84],
    ["sep-22", 3.26, 2.4], ["oct-22", 3.27, 1.34], ["nov-22", 2.31, 1.49], ["dec-22", 2.31, 1.79],
    ["jan-23", 2.59, 1.44], ["feb-23", 2.44, 1.32], ["mar-23", 2.31, 1.21], ["apr-23", 1.78, 1.19],
    ["may-23", 1.54, 1.05], ["jun-23", 1.48, 1.05], ["jul-23", 1.29, 1.03], ["aug-23", 1.26, 1.07],
    ["sep-23", 1.26, 1.11], ["oct-23", 1.24, 1.19], ["nov-23", 1.27, 1.18], ["dec-23", 1.29, 1.09],
    ["jan-24", 1.4, 1.12], ["feb-24", 1.4, 1.07], ["mar-24", 1.38, 1.07], ["apr-24", 1.34, 1.0],
    ["may-24", 1.3, 1.11], ["jun-24", 1.29, 1.16], ["jul-24", 1.29, 1.15], ["aug-24", 1.28, 1.2], ["sep-24", 1.29, 1.19],
    ["oct-24", 1.32, 1.25], ["nov-24", 1.32, 1.28], ["dec-24", 1.33, 1.29], ["jan-25", 1.33, 1.33],
    ["feb-25", 1.34, 1.35], ["mar-25", 1.35, 1.25]
]

LOG_PATH = os.path.join(OUTPUT_DIR, 'fetch_prices.log')

def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)


def month_str(dt):
    return dt.strftime('%Y-%m')


def get_monthly_avg(df, value_col='price'):
    """
    Groepeer op maand en neem het gemiddelde van value_col.
    Verwacht een kolom 'time' (datetime64) en value_col (float).
    """
    df = df.copy()
    df['month'] = df['time'].dt.strftime('%Y-%m')
    return df.groupby('month')[value_col].mean().reset_index()


def build_monthly_csv(df_monthly, cbs_rates, procurement_costs, type_):
    """
    Combineer kale maandgemiddelden met CBS-tax en procurement_costs.
    type_ = 'electricity' of 'gas'
    Geeft DataFrame met kolommen: month, base_price, energy_tax, procurement_costs, total_price
    """
    rows = []
    for _, row in df_monthly.iterrows():
        month = row['month']
        base_price = row['price']
        # Zoek juiste CBS rate
        cbs = next((r for r in cbs_rates if r['period'] == month), None)
        if not cbs:
            print(f"[WARN] Geen CBS-data voor maand {month}, overslaan.")
            continue
        if type_ == 'electricity':
            energy_tax = cbs.get('energy_tax')
        else:
            energy_tax = cbs.get('gas_energy_tax')
        if energy_tax is None:
            print(f"[WARN] Geen energiebelasting voor {type_} in maand {month}, overslaan.")
            continue
        total_price = base_price + energy_tax + procurement_costs
        rows.append({
            'month': month,
            'base_price': round(base_price, 5),
            'energy_tax': round(energy_tax, 5),
            'procurement_costs': round(procurement_costs, 5),
            'total_price': round(total_price, 5)
        })
    return pd.DataFrame(rows)


def log_run(message):
    ensure_output_dir()
    with open(LOG_PATH, 'a') as f:
        f.write(f"[{datetime.now().isoformat()}] {message}\n")


def file_hash(path):
    if not os.path.exists(path):
        return None
    with open(path, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()


def main():
    """
    Haal CBS-data, kale prijzen en schrijf maandgemiddelden naar CSV's.
    Vergelijk CBS- en dynamische tarieven in één bestand.
    """
    # Define helper function for date comparison
    def month_gt_mar25(date_str):
        try:
            # Check the format of the string
            if isinstance(date_str, str):
                if len(date_str) == 6 and '-' in date_str:  # Format like 'apr-25' or 'Apr-25'
                    # Zorg ervoor dat de maand lowercase is voor consistentie
                    if not date_str[0].islower():
                        date_str = date_str[0].lower() + date_str[1:]
                    dt = datetime.strptime(date_str, '%b-%y')
                elif len(date_str) == 7 and '-' in date_str:  # Format like '2025-04'
                    dt = datetime.strptime(date_str, '%Y-%m')
                else:
                    return False
            else:
                return False
            return dt > datetime(2025, 3, 1)
        except Exception:
            return False
    
    print("[INFO] Ophalen CBS-tarieven...")
    cbs_rates = get_cbs_rates()
    print(f"[INFO] {len(cbs_rates)} CBS-maanden opgehaald.")

    print("[INFO] Ophalen kale elektriciteitsprijzen...")
    start_jan21 = datetime(2021, 1, 1)
    elec_df = get_dynamic_electricity_prices(start_date=start_jan21)
    if elec_df is None or elec_df.empty:
        print("[ERROR] Geen elektriciteitsprijzen gevonden!")
        return
    print(f"[INFO] {len(elec_df)} elektriciteitsprijzen opgehaald.")

    print("[INFO] Ophalen kale gasprijzen...")
    gas_df = get_dynamic_gas_prices(start_date=start_jan21)
    if gas_df is None or gas_df.empty:
        print("[ERROR] Geen gasprijzen gevonden!")
        return
    print(f"[INFO] {len(gas_df)} gasprijzen opgehaald.")

    # Maandgemiddelden
    elec_monthly = get_monthly_avg(elec_df)
    gas_monthly = get_monthly_avg(gas_df)

    # Combineer met CBS en procurement_costs
    elec_out = build_monthly_csv(elec_monthly, cbs_rates, PROCUREMENT_COSTS_ELEC, 'electricity')
    gas_out = build_monthly_csv(gas_monthly, cbs_rates, PROCUREMENT_COSTS_GAS, 'gas')

    ensure_output_dir()
    elec_path = os.path.join(OUTPUT_DIR, 'monthly_electricity_prices.csv')
    gas_path = os.path.join(OUTPUT_DIR, 'monthly_gas_prices.csv')
    elec_out.to_csv(elec_path, index=False)
    gas_out.to_csv(gas_path, index=False)
    print(f"[INFO] CSV's geschreven naar {elec_path} en {gas_path}")

    # --- Vergelijkingstabel maken ---
    # Originele dataframes
    orig_elec_df = pd.DataFrame(ORIG_ELEC, columns=["DATE", "CBS stroom", "ANWB stroom"])
    orig_gas_df = pd.DataFrame(ORIG_GAS, columns=["DATE", "CBS gas", "ANWB gas"])
    # Pipeline dataframes (vanaf apr-25)
    cbs_df = pd.DataFrame([
        {
            'DATE': r['period'],
            'CBS stroom': r.get('total'),
            'CBS gas': r.get('gas_total')
        }
        for r in cbs_rates
    ])
    anwb_elec = elec_out[['month', 'total_price']].rename(columns={'month': 'DATE', 'total_price': 'ANWB stroom'})
    anwb_gas = gas_out[['month', 'total_price']].rename(columns={'month': 'DATE', 'total_price': 'ANWB gas'})
    # Merge pipeline data
    pipeline = cbs_df.merge(anwb_elec, on='DATE', how='outer').merge(anwb_gas, on='DATE', how='outer')
    pipeline = pipeline.sort_values('DATE').reset_index(drop=True)
    
    # Pipeline data: alles na mar-25, met mooie maandnotatie
    pipeline['DATE'] = pipeline['DATE'].apply(
        lambda d: datetime.strptime(d, '%Y-%m').strftime('%b-%y').lower() 
        if isinstance(d, str) and len(d) == 7 and '-' in d and d[4] == '-'  # Format like '2025-04'
        else d
    )
    pipeline = pipeline[pipeline['DATE'].apply(month_gt_mar25)]

    # Combineer origineel + pipeline
    compare = pd.concat([orig_elec_df.join(orig_gas_df.set_index('DATE'), on='DATE', rsuffix='_gas').iloc[:, :5], pipeline], ignore_index=True, sort=False)
    # Schrijf naar CSV
    compare_path = os.path.join(OUTPUT_DIR, 'compare_prices.csv')
    prev_hash = file_hash(compare_path)
    compare.to_csv(compare_path, index=False, float_format='%.4f')
    new_hash = file_hash(compare_path)
    changed = (prev_hash != new_hash)
    log_run(f"Run: {len(compare)} maanden in compare_prices.csv. Gewijzigd: {changed}")
    print(f"[INFO] Vergelijkings-CSV geschreven naar {compare_path}")

if __name__ == '__main__':
    main()

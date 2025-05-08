import os
import pandas as pd
from fetch_cbs import get_cbs_rates
from fetch_dynamic_electricity import get_dynamic_electricity_prices
from fetch_dynamic_gas import get_dynamic_gas_prices
from datetime import datetime

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
PROCUREMENT_COSTS_ELEC = 0.04 * 1.21
PROCUREMENT_COSTS_GAS = 0.05911 * 1.21


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


def main():
    """
    Haal CBS-data, kale prijzen en schrijf maandgemiddelden naar CSV's.
    Vergelijk CBS- en dynamische tarieven in één bestand.
    """
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
    # CBS-data per maand extraheren
    cbs_df = pd.DataFrame([
        {
            'DATE': r['period'],
            'CBS stroom': r.get('total'),
            'CBS gas': r.get('gas_total')
        }
        for r in cbs_rates
    ])
    # ANWB-data per maand extraheren
    anwb_elec = elec_out[['month', 'total_price']].rename(columns={'month': 'DATE', 'total_price': 'ANWB stroom'})
    anwb_gas = gas_out[['month', 'total_price']].rename(columns={'month': 'DATE', 'total_price': 'ANWB gas'})
    # Merge alles op maand
    compare = cbs_df.merge(anwb_elec, on='DATE', how='outer').merge(anwb_gas, on='DATE', how='outer')
    # Sorteer op maand
    compare = compare.sort_values('DATE').reset_index(drop=True)
    # Optioneel: mooie maandnotatie (jan-21)
    def to_nl_month(date_str):
        try:
            dt = datetime.strptime(date_str, '%Y-%m')
            return dt.strftime('%b-%y')
        except:
            return date_str
    compare['DATE'] = compare['DATE'].apply(to_nl_month)
    # Schrijf naar CSV
    compare_path = os.path.join(OUTPUT_DIR, 'compare_prices.csv')
    compare.to_csv(compare_path, index=False, float_format='%.4f')
    print(f"[INFO] Vergelijkings-CSV geschreven naar {compare_path}")

if __name__ == '__main__':
    main()

import re

def check_month_consistency(orig_elec, orig_gas):
    elec_months = [row[0] for row in orig_elec]
    gas_months = [row[0] for row in orig_gas]

    # Check op dubbele maanden
    dups_elec = set([m for m in elec_months if elec_months.count(m) > 1])
    dups_gas = set([m for m in gas_months if gas_months.count(m) > 1])
    if dups_elec:
        print(f"[CHECK] Dubbele maanden in ORIG_ELEC: {dups_elec}")
    if dups_gas:
        print(f"[CHECK] Dubbele maanden in ORIG_GAS: {dups_gas}")

    # Check op missende maanden
    only_in_elec = set(elec_months) - set(gas_months)
    only_in_gas = set(gas_months) - set(elec_months)
    if only_in_elec:
        print(f"[CHECK] Maanden alleen in ORIG_ELEC: {only_in_elec}")
    if only_in_gas:
        print(f"[CHECK] Maanden alleen in ORIG_GAS: {only_in_gas}")

    # Check op maandnotatie
    month_re = re.compile(r'^[a-z]{3}-\d{2}$')
    bad_elec = [m for m in elec_months if not month_re.match(m)]
    bad_gas = [m for m in gas_months if not month_re.match(m)]
    if bad_elec:
        print(f"[CHECK] Foute maandnotatie in ORIG_ELEC: {bad_elec}")
    if bad_gas:
        print(f"[CHECK] Foute maandnotatie in ORIG_GAS: {bad_gas}")

    # Check op sortering
    def month_key(m):
        try:
            return datetime.strptime(m, '%b-%y')
        except Exception:
            return None
    sorted_elec = sorted(elec_months, key=month_key)
    sorted_gas = sorted(gas_months, key=month_key)
    if elec_months != sorted_elec:
        print("[CHECK] ORIG_ELEC is niet gesorteerd op maand!")
    if gas_months != sorted_gas:
        print("[CHECK] ORIG_GAS is niet gesorteerd op maand!")

check_month_consistency(ORIG_ELEC, ORIG_GAS)
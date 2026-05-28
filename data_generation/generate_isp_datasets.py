"""
Synthetic ISP Dataset Generator
Modelled on a large Australian ISP (e.g. Superloop):
  - 100,000+ km fibre, 650k residential, 97k business, 198k wholesale services

Outputs:
  customers.csv          ~2,000 rows
  network_telemetry.csv  ~50,000 rows
  support_tickets.csv    ~3,000 rows
  billing_events.csv     ~15,000 rows

Dependencies: pandas, numpy, faker, random (all stdlib/common)
"""

import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import date, timedelta

fake = Faker('en_AU')
rng = np.random.default_rng(42)
random.seed(42)

TODAY = date.today()

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

REGIONS = ['Sydney', 'Melbourne', 'Brisbane', 'Adelaide', 'Perth', 'Canberra']
REGION_WEIGHTS = [0.32, 0.26, 0.20, 0.08, 0.10, 0.04]  # ~ABS population shares

CUSTOMER_TYPES = ['residential', 'business', 'wholesale']
CUSTOMER_TYPE_WEIGHTS = [0.65, 0.20, 0.15]

PLAN_BY_TYPE = {
    'residential': {
        'plans':   ['NBN50', 'NBN100', 'NBN250', 'NBN1000'],
        'weights': [0.25, 0.40, 0.25, 0.10],
    },
    'business': {
        'plans':   ['NBN100', 'NBN250', 'NBN1000', 'Fibre', 'Business_VoIP'],
        'weights': [0.20, 0.25, 0.25, 0.20, 0.10],
    },
    'wholesale': {
        'plans':   ['Wholesale_Dark_Fibre', 'Fibre', 'NBN1000'],
        'weights': [0.60, 0.25, 0.15],
    },
}

# Base monthly price (AUD) per plan — realistic 2024 market rates
PLAN_PRICE = {
    'NBN50':               59.00,
    'NBN100':              79.00,
    'NBN250':              99.00,
    'NBN1000':            119.00,
    'Fibre':              299.00,
    'Business_VoIP':      149.00,
    'Wholesale_Dark_Fibre': 899.00,
}

# Typical bandwidth (GB/day) distribution params [mean, std] by plan
PLAN_BW = {
    'NBN50':               (8,   3),
    'NBN100':              (18,  6),
    'NBN250':              (40, 12),
    'NBN1000':             (90, 25),
    'Fibre':               (150, 40),
    'Business_VoIP':       (5,   2),
    'Wholesale_Dark_Fibre': (400, 80),
}

# Peak-hour latency [mean_ms, std_ms] per region (congestion varies)
REGION_LATENCY = {
    'Sydney':    (12, 4),
    'Melbourne': (11, 3),
    'Brisbane':  (14, 5),
    'Adelaide':  (18, 6),
    'Perth':     (22, 7),   # higher due to distance
    'Canberra':  (10, 3),
}

# 20 network nodes spread across regions
NODES = {r: [f'NODE-{i:02d}' for i in ids]
         for r, ids in {
             'Sydney':    range(1, 6),
             'Melbourne': range(6, 10),
             'Brisbane':  range(10, 13),
             'Adelaide':  range(13, 15),
             'Perth':     range(15, 18),
             'Canberra':  range(18, 21),
         }.items()}

N_CUSTOMERS = 2_000
N_TELEMETRY_CUSTOMERS = 600
TELEMETRY_DAYS = 90
N_TICKETS = 3_000
N_BILLING_MONTHS = 12

# ─────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────

def weighted_choice(options, weights, size=1):
    weights = np.array(weights, dtype=float)
    weights /= weights.sum()
    idx = rng.choice(len(options), size=size, p=weights)
    return [options[i] for i in idx] if size > 1 else options[idx[0]]


def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def month_start(d: date) -> date:
    return d.replace(day=1)


# ─────────────────────────────────────────────
# DATASET 1: CUSTOMERS
# ─────────────────────────────────────────────

def build_customers() -> pd.DataFrame:
    print("Building customers.csv …")
    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        ctype = weighted_choice(CUSTOMER_TYPES, CUSTOMER_TYPE_WEIGHTS)
        region = weighted_choice(REGIONS, REGION_WEIGHTS)
        plan_cfg = PLAN_BY_TYPE[ctype]
        plan = weighted_choice(plan_cfg['plans'], plan_cfg['weights'])

        base_price = PLAN_PRICE[plan]
        charge = round(base_price + rng.normal(0, base_price * 0.03), 2)

        # Contract type — wholesale almost always long-term
        if ctype == 'wholesale':
            contract = weighted_choice(['month-to-month', '1-year', '2-year'], [0.05, 0.20, 0.75])
        elif ctype == 'business':
            contract = weighted_choice(['month-to-month', '1-year', '2-year'], [0.25, 0.40, 0.35])
        else:
            contract = weighted_choice(['month-to-month', '1-year', '2-year'], [0.40, 0.35, 0.25])

        tenure = int(np.clip(rng.exponential(scale=24), 1, 60))  # skewed toward longer

        onboarded = random_date(date(2020, 1, 1), TODAY - timedelta(days=tenure * 30))

        # Churn probability
        base_churn = 0.15
        if contract == 'month-to-month':
            base_churn *= 1.8
        elif contract == '2-year':
            base_churn *= 0.4
        if ctype == 'business':
            base_churn *= 0.7
        churned = random.random() < min(base_churn, 0.40)

        churn_date = None
        if churned:
            churn_date = random_date(
                onboarded + timedelta(days=30),
                min(onboarded + timedelta(days=tenure * 30), TODAY)
            )

        # Add-ons
        cybersec = random.random() < (0.55 if ctype == 'business' else 0.10)
        voip = random.random() < (0.45 if ctype == 'business' else 0.08 if ctype == 'residential' else 0.20)

        rows.append({
            'customer_id':             f'CUST-{i:05d}',
            'customer_type':           ctype,
            'region':                  region,
            'plan_type':               plan,
            'contract_type':           contract,
            'monthly_charge_aud':      charge,
            'tenure_months':           tenure,
            'has_cybersecurity_addon': cybersec,
            'has_voip_addon':          voip,
            'onboarded_date':          onboarded,
            'churn_flag':              churned,
            'churn_date':              churn_date,
        })

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# DATASET 2: NETWORK TELEMETRY
# ─────────────────────────────────────────────

# Define 3 outage events: (start_offset_days_ago, duration_days, affected_regions)
OUTAGE_EVENTS = [
    {'start': TODAY - timedelta(days=75), 'end': TODAY - timedelta(days=73), 'regions': ['Sydney', 'Melbourne']},
    {'start': TODAY - timedelta(days=45), 'end': TODAY - timedelta(days=44), 'regions': ['Brisbane']},
    {'start': TODAY - timedelta(days=15), 'end': TODAY - timedelta(days=14), 'regions': ['Perth', 'Adelaide']},
]


def is_in_outage(d: date, region: str) -> bool:
    for evt in OUTAGE_EVENTS:
        if evt['start'] <= d <= evt['end'] and region in evt['regions']:
            return True
    return False


def build_telemetry(customers: pd.DataFrame) -> pd.DataFrame:
    print("Building network_telemetry.csv …")

    # Sample 600 customers for telemetry
    sample_ids = customers['customer_id'].sample(N_TELEMETRY_CUSTOMERS, random_state=42).tolist()
    cust_map = customers.set_index('customer_id')[['region', 'plan_type', 'churn_flag', 'churn_date']].to_dict('index')

    rows = []
    telemetry_id = 1
    date_range = [TODAY - timedelta(days=TELEMETRY_DAYS - 1 - d) for d in range(TELEMETRY_DAYS)]

    for cid in sample_ids:
        info = cust_map[cid]
        region = info['region']
        plan = info['plan_type']
        is_churner = info['churn_flag']
        churn_dt = info['churn_date']
        node = random.choice(NODES[region])
        lat_mean, lat_std = REGION_LATENCY[region]
        bw_mean, bw_std = PLAN_BW.get(plan, (20, 7))

        for d in date_range:
            # Stop telemetry after churn
            if is_churner and churn_dt and d > churn_dt:
                continue

            in_outage = is_in_outage(d, region)

            # Pre-churn degradation: ramp up issues in last 30 days before churn
            days_to_churn = None
            degradation = 1.0
            if is_churner and churn_dt:
                days_to_churn = (churn_dt - d).days
                if 0 <= days_to_churn <= 30:
                    degradation = 1 + (30 - days_to_churn) / 30 * 1.5  # up to 2.5x

            # Bandwidth
            bw = max(0.1, rng.normal(bw_mean, bw_std) * (0.6 if in_outage else 1.0))

            # Latency
            peak_lat = max(1, rng.normal(lat_mean * degradation, lat_std) * (rng.uniform(3, 6) if in_outage else 1.0))
            offpeak_lat = max(1, peak_lat * rng.uniform(0.4, 0.7))

            # Packet loss
            if in_outage:
                pkt_loss = round(rng.uniform(2.5, 6.0), 2)
            else:
                pkt_loss = round(max(0, rng.exponential(0.3) * degradation), 2)
                pkt_loss = min(pkt_loss, 5.0)

            # Connection drops
            if in_outage:
                drops = int(rng.integers(2, 6))
            else:
                drops = int(np.clip(rng.poisson(0.3 * degradation), 0, 5))

            # SLA breach — business/wholesale more sensitive (threshold lower)
            sla_threshold_lat = 80 if info.get('plan_type') in ('Business_VoIP', 'Fibre', 'Wholesale_Dark_Fibre') else 100
            sla_breach = (peak_lat > sla_threshold_lat) or (pkt_loss > 3.0)

            rows.append({
                'telemetry_id':          telemetry_id,
                'customer_id':           cid,
                'date':                  d,
                'region':                region,
                'plan_type':             plan,
                'daily_bandwidth_gb':    round(bw, 2),
                'peak_hour_latency_ms':  round(peak_lat, 1),
                'off_peak_latency_ms':   round(offpeak_lat, 1),
                'packet_loss_percent':   pkt_loss,
                'connection_drops':      drops,
                'sla_breach':            sla_breach,
                'outage_flag':           in_outage,
                'node_id':               node,
            })
            telemetry_id += 1

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# DATASET 3: SUPPORT TICKETS
# ─────────────────────────────────────────────

ISSUE_CATEGORIES = ['connectivity_drop', 'slow_speed', 'billing_query',
                    'hardware_fault', 'outage_report', 'provisioning_delay']
ISSUE_WEIGHTS    = [0.30, 0.25, 0.20, 0.10, 0.10, 0.05]

CHANNELS = ['phone', 'email', 'chat', 'web_portal']
CHANNEL_WEIGHTS = [0.35, 0.25, 0.20, 0.20]


def build_tickets(customers: pd.DataFrame) -> pd.DataFrame:
    print("Building support_tickets.csv …")

    cust_ids = customers['customer_id'].tolist()
    cust_map = customers.set_index('customer_id')[['region']].to_dict('index')

    # Build a lookup of "outage windows" → extra ticket volume
    outage_windows = [(e['start'], e['end'], e['regions']) for e in OUTAGE_EVENTS]

    rows = []
    window_start = TODAY - timedelta(days=180)

    # Distribute base tickets uniformly; then inject spikes near outage dates
    for i in range(1, N_TICKETS + 1):
        cid = random.choice(cust_ids)
        region = cust_map[cid]['region']

        open_d = random_date(window_start, TODAY - timedelta(days=1))

        # Outage spike: if this ticket's region had an outage, bias dates toward outage window
        for o_start, o_end, o_regions in outage_windows:
            if region in o_regions and random.random() < 0.15:
                # Pull date toward outage window ± 3 days
                spike_start = max(window_start, o_start - timedelta(days=3))
                spike_end   = min(TODAY - timedelta(days=1), o_end + timedelta(days=3))
                open_d = random_date(spike_start, spike_end)
                break

        resolution_days = random.randint(1, 14)
        is_resolved = random.random() < 0.82
        close_d = open_d + timedelta(days=resolution_days) if is_resolved else None
        if close_d and close_d > TODAY:
            close_d = None
            is_resolved = False

        category = weighted_choice(ISSUE_CATEGORIES, ISSUE_WEIGHTS)
        channel   = weighted_choice(CHANNELS, CHANNEL_WEIGHTS)

        sat = None
        if is_resolved:
            # Faster resolution → higher satisfaction
            if resolution_days <= 2:
                sat = weighted_choice([3, 4, 5], [0.10, 0.30, 0.60])
            elif resolution_days <= 7:
                sat = weighted_choice([2, 3, 4, 5], [0.10, 0.25, 0.40, 0.25])
            else:
                sat = weighted_choice([1, 2, 3, 4], [0.20, 0.35, 0.30, 0.15])

        rows.append({
            'ticket_id':          f'TKT-{i:05d}',
            'customer_id':        cid,
            'open_date':          open_d,
            'close_date':         close_d,
            'issue_category':     category,
            'region':             region,
            'channel':            channel,
            'resolved':           is_resolved,
            'satisfaction_score': sat,
        })

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# DATASET 4: BILLING EVENTS
# ─────────────────────────────────────────────

PAYMENT_METHODS = ['direct_debit', 'credit_card', 'bank_transfer']
PAYMENT_METHOD_WEIGHTS = [0.55, 0.30, 0.15]


def build_billing(customers: pd.DataFrame) -> pd.DataFrame:
    print("Building billing_events.csv …")

    rows = []
    billing_id = 1

    # Generate one record per customer per month for last 12 months
    billing_months = [month_start(TODAY - timedelta(days=30 * m)) for m in range(N_BILLING_MONTHS - 1, -1, -1)]

    for _, cust in customers.iterrows():
        cid = cust['customer_id']
        base_charge = cust['monthly_charge_aud']
        method = weighted_choice(PAYMENT_METHODS, PAYMENT_METHOD_WEIGHTS)

        # Churn customers: stop billing after churn_date
        churn_dt = cust['churn_date'] if cust['churn_flag'] else None

        for bm in billing_months:
            if churn_dt and bm > churn_dt:
                continue  # no bill after churn

            amount = round(base_charge + rng.normal(0, 1.5), 2)  # minor variance (credits, overages)
            amount = max(0, amount)

            # Payment status — late payers somewhat consistent
            r = random.random()
            if r < 0.85:
                status = 'paid'
            elif r < 0.95:
                status = 'late'
            else:
                status = 'failed'

            pay_date = None
            if status == 'paid':
                pay_date = bm + timedelta(days=random.randint(1, 7))
            elif status == 'late':
                pay_date = bm + timedelta(days=random.randint(8, 30))

            rows.append({
                'billing_id':       billing_id,
                'customer_id':      cid,
                'billing_month':    bm,
                'amount_charged_aud': amount,
                'payment_status':   status,
                'payment_date':     pay_date,
                'payment_method':   method,
            })
            billing_id += 1

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == '__main__':
    customers  = build_customers()
    telemetry  = build_telemetry(customers)
    tickets    = build_tickets(customers)
    billing    = build_billing(customers)

    customers.to_csv('customers.csv', index=False)
    print(f"  → customers.csv        {len(customers):,} rows")

    telemetry.to_csv('network_telemetry.csv', index=False)
    print(f"  → network_telemetry.csv {len(telemetry):,} rows")

    tickets.to_csv('support_tickets.csv', index=False)
    print(f"  → support_tickets.csv   {len(tickets):,} rows")

    billing.to_csv('billing_events.csv', index=False)
    print(f"  → billing_events.csv    {len(billing):,} rows")

    print("\nAll datasets generated successfully.")

"""Generate synthetic labelled banking transactions for Redshift ML fraud training."""
import csv, random
from datetime import datetime, timedelta
random.seed(42)
CUST=["Ahmed K.","Fatima R.","Omar S.","Layla M.","Khalid B.","Sara T.","Yousef H.","Noura A."]
TYPES=["card_payment","atm_withdrawal","wire_transfer","mobile_payment","loan_repayment","fx_exchange","check_deposit","bill_payment"]
CH=["mobile_app","atm","branch","online_banking","pos_terminal","phone_banking"]
LOC=["Dubai","Abu Dhabi","Riyadh","Amman","Cairo","Doha","Manama","Kuwait City","Muscat"]
FLOC=["Lagos","London","New York","Mumbai","Sao Paulo"]
CUR=["AED","USD","EUR","GBP","SAR","JOD"]
rows=[]; base=datetime(2026,1,1)
for _ in range(600):
    fraud=random.random()<0.08
    ts=base+timedelta(days=random.randint(0,258),hours=random.randint(0,23),minutes=random.randint(0,59))
    age=random.randint(1,360); score=random.randint(300,850)
    t=random.choice(TYPES); ch=random.choice(CH)
    if fraud:
        amt=round(random.uniform(5000,48000),2); loc=random.choice(FLOC+["Dubai"]); bal=random.randint(500,15000)
        if random.random()<0.6: age=random.randint(1,12); score=random.randint(300,580)
        t=random.choice(["wire_transfer","card_payment","atm_withdrawal"]); ch=random.choice(["online_banking","atm","mobile_app"])
    else:
        amt=round(random.uniform(10,12000),2); loc=random.choice(LOC); bal=random.randint(2000,95000)
    h=ts.hour
    rows.append(dict(txn_id=random.randint(100000,999999),txn_timestamp=ts.strftime("%Y-%m-%d %H:%M:%S"),
        customer_name=random.choice(CUST),txn_type=t,amount=amt,currency=random.choice(CUR),channel=ch,location=loc,
        account_id=random.randint(100000000,999999999),account_age_months=age,avg_balance=bal,credit_score=score,
        amt_to_balance_ratio=round(amt/max(bal,1),4),is_international=int(loc in FLOC),
        is_high_risk_channel=int(ch in ("online_banking","atm")),hour_of_day=h,is_weekend=int(ts.weekday()>=5),
        is_night=int(h<6 or h>22),is_fraud=int(fraud)))
with open("data/banking_training_data.csv","w",newline="") as f:
    w=csv.DictWriter(f,fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
print(f"{len(rows)} rows, {sum(r['is_fraud'] for r in rows)} fraud")

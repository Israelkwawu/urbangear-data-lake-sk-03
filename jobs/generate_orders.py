"""Generate one day of UrbanGear raw orders as JSON Lines and upload to MinIO.

Same 17-field schema as the SK-03 guided lab. Deterministic: the same date and
dirty rate always produce exactly the same file (the random seed is the date).

Usage:
    python generate_orders.py --process_date 2026-07-01
    python generate_orders.py --process_date 2026-07-05 --dirty_rate 0.12   # a "bad day"
"""
import argparse
import json
import os
import random
from datetime import datetime, timedelta

import boto3

BUCKET = "urbangear-data-lake"
ORDERS_PER_DAY = 1000            # each order has 1-3 lines -> ~2,000 lines/day

PLATFORMS = ["website", "mobile_app", "marketplace"]
PRODUCTS = [  # (product_id, product_name, category, unit_price)
    ("P100", "Trail Backpack", "Bags",      89.99),
    ("P200", "Camp Stove",     "Cooking",   45.50),
    ("P300", "Headlamp",       "Lighting",  19.99),
    ("P400", "Tent 2P",        "Shelter",  199.00),
    ("P500", "Sleeping Bag",   "Sleeping", 129.00),
    ("P600", "Water Filter",   "Hydration", 34.95),
]
PAYMENTS = ["card", "paypal", "apple_pay", "gift_card"]
LOCATIONS = [  # (country, state, city)
    ("US", "CA", "Los Angeles"), ("US", "NY", "New York"), ("US", "TX", "Austin"),
    ("CA", "ON", "Toronto"),     ("GB", "ENG", "London"),
]
DIRT_TYPES = ["null_customer", "negative_qty", "future_date", "bad_platform", "corrupt_json"]


def make_lines(process_date: str, dirty_rate: float) -> list[str]:
    rng = random.Random(f"{process_date}-{dirty_rate}")   # deterministic per (date, rate)
    day = datetime.strptime(process_date, "%Y-%m-%d")
    lines = []
    for i in range(ORDERS_PER_DAY):
        order_id = f"ORD-{process_date.replace('-', '')}-{i:05d}"
        customer_id = f"CUST-{rng.randint(1, 400):04d}"
        platform = rng.choice(PLATFORMS)
        if rng.random() < 0.10:                       # messy-but-valid casing/spacing
            platform = f" {platform.title()} "
        order_ts = day + timedelta(seconds=rng.randint(0, 86_399))
        country, state, city = rng.choice(LOCATIONS)
        payment = rng.choice(PAYMENTS)
        for product_id, name, category, price in rng.sample(PRODUCTS, rng.randint(1, 3)):
            qty = rng.randint(1, 4)
            rec = {
                "order_id": order_id,
                "customer_id": customer_id,
                "platform": platform,
                "order_date": order_ts.strftime("%Y-%m-%dT%H:%M:%S"),
                "product_id": product_id,
                "product_name": name,
                "category": category,
                "quantity": qty,
                "unit_price": price,
                "discount_pct": rng.choice([0.0, 0.0, 0.0, 5.0, 10.0, 15.0, None]),
                "shipping_cost": round(rng.uniform(0, 15), 2),
                "tax_amount": round(qty * price * 0.08, 2),
                "payment_method": payment,
                "shipping_country": country.lower() if rng.random() < 0.05 else country,
                "shipping_state": state,
                "shipping_city": city,
                "is_returned": rng.random() < 0.03,
            }
            line = json.dumps(rec)
            # --- inject realistic dirt into a small share of lines ---
            if rng.random() < dirty_rate:
                dirt = rng.choice(DIRT_TYPES)
                if dirt == "null_customer":
                    rec["customer_id"] = None
                elif dirt == "negative_qty":
                    rec["quantity"] = -rng.randint(1, 3)
                elif dirt == "future_date":
                    rec["order_date"] = "2099-01-01T00:00:00"
                elif dirt == "bad_platform":
                    rec["platform"] = "fax"
                line = json.dumps(rec)
                if dirt == "corrupt_json":
                    line = line[:-20]                 # truncated line = broken JSON
            lines.append(line)
    return lines


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--process_date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--dirty_rate", type=float, default=0.03)
    args = parser.parse_args()

    lines = make_lines(args.process_date, args.dirty_rate)
    body = ("\n".join(lines) + "\n").encode("utf-8")

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )
    key = f"raw/orders/dt={args.process_date}/part-00000.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    print(f"uploaded s3://{BUCKET}/{key} ({len(lines)} lines, {len(body):,} bytes)")


if __name__ == "__main__":
    main()

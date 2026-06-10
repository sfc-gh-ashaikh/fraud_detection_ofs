# Feature Catalogue: Fraud Detection Model

Full specification of all 170+ features used by the fraud detection model, grouped by entity and computation method.

## 1. Transaction Attributes (9 features)

Raw properties of the current transaction. No aggregation needed.

| Feature | Description | Encoding |
|---------|-------------|----------|
| `purchase_amount` | Transaction amount | Log-transformed |
| `local_currency_code` | Currency of the transaction | Target-encoded |
| `merchant_country` | Merchant country code | Target-encoded |
| `mcc_code` | Merchant Category Code | Target-encoded |
| `tap_and_pay_type` | Contactless method (NFC, IN_APP, etc.) | Target-encoded |
| `wallet_device_type` | Device type (IPHONE, ANDROID, WATCH, etc.) | Target-encoded |
| `wallet_name` | Wallet provider (Apple Pay, Google Pay, Samsung Pay, MRCHTOKEN) | Target-encoded |
| `authenticated_3ds_challenge_flag` | Whether 3DS challenge was triggered | Binary |
| `is_merchant_initiated_purchase` | Whether merchant initiated the purchase | Binary |

## 2. Customer Profile Features (13 features)

Static and lifetime-aggregate features. Served via Online Feature Store (batch feature view, daily refresh).

| Feature | Description |
|---------|-------------|
| `customer_age_at_transaction` | Customer's age in years at transaction time |
| `days_since_first_purchase` | Days since first-ever purchase |
| `days_since_last_purchase` | Days since most recent prior purchase |
| `days_since_last_purchase_missing` | 1 if no prior purchases |
| `days_since_first_purchase_missing` | 1 if no prior purchases |
| `num_purchases` | Total lifetime purchase count |
| `num_gbr_purchases` | Lifetime GBR (UK) purchase count |
| `num_nongbr_purchases` | Lifetime international purchase count |
| `distinct_wallet_dpans` | Distinct wallet DPANs used historically |
| `purchase_total` | Lifetime total spend (log-transformed) |
| `avg_purchase_amount` | Lifetime average amount (log-transformed) |
| `min_purchase_amount` | Lifetime minimum amount (log-transformed) |
| `max_purchase_amount` | Lifetime maximum amount (log-transformed) |

## 3. Customer Transaction Velocity (65 features)

Rolling-window aggregates per customer. Computed via Online Feature Store stream aggregation (CONTINUOUS).

**Time windows**: 1hr, 6hr, 24hr, 48hr, 1wk

| Feature Group | Metric | x5 windows | Description |
|---------------|--------|-----------|-------------|
| `purchases_num_*` | COUNT | 5 | Number of purchases |
| `purchases_gbr_num_*` | COUNT | 5 | GBR purchases |
| `purchases_nongbr_num_*` | COUNT | 5 | Non-GBR purchases |
| `purchases_sum_*` | SUM | 5 | Total spend |
| `purchases_min_*` | MIN | 5 | Minimum amount |
| `purchases_max_*` | MAX | 5 | Maximum amount |
| `distinct_purchase_amt_*` | COUNT DISTINCT | 5 | Distinct amounts (card testing signal) |
| `cust_merchant_purchases_num_*` | COUNT | 5 | Same-merchant purchases |
| `cust_merchant_purchases_sum_*` | SUM | 5 | Same-merchant spend |
| `distinct_card_tokens_*` | COUNT DISTINCT | 5 | Distinct cards used |
| `distinct_wallet_dpan_*` | COUNT DISTINCT | 5 | Distinct DPANs used |
| `distinct_merchants_*` | COUNT DISTINCT | 5 | Distinct merchants |
| `distinct_countries_*` | COUNT DISTINCT | 2 (24h, 1wk) | Distinct countries |

## 4. Merchant Velocity (20 features)

Rolling aggregates at the merchant level (across all customers). Computed via Online Feature Store stream aggregation (CONTINUOUS).

| Feature Group | Metric | x5 windows | Description |
|---------------|--------|-----------|-------------|
| `merchant_purchases_num_*` | COUNT | 5 | Total transactions at merchant |
| `merchant_purchases_sum_*` | SUM | 5 | Total spend at merchant |
| `merchant_unique_customers_*` | COUNT DISTINCT | 5 | Distinct customers |
| `merchant_max_purchase_amount_*` | MAX | 5 | Largest single transaction |

## 5. Wallet DPAN Velocity (15 features)

Rolling aggregates at the DPAN level. Computed via Online Feature Store stream aggregation (CONTINUOUS).

| Feature Group | Metric | x5 windows | Description |
|---------------|--------|-----------|-------------|
| `dpan_purchases_num_*` | COUNT | 5 | Transactions using this DPAN |
| `dpan_purchases_sum_*` | SUM | 5 | Total spend on this DPAN |
| `dpan_distinct_customers_*` | COUNT DISTINCT | 5 | Distinct customers (shared DPAN = fraud signal) |

## 6. IP Address Velocity (12 features)

Rolling aggregates at the IP level. Computed via Online Feature Store stream aggregation (CONTINUOUS).

| Feature Group | Metric | Windows | Description |
|---------------|--------|---------|-------------|
| `ip_purchases_num_*` | COUNT | 5 (all) | Transactions from this IP |
| `ip_purchases_sum_*` | SUM | 2 (24h, 1wk) | Total spend from IP |
| `ip_distinct_customers_*` | COUNT DISTINCT | 5 (all) | Distinct customers (shared IP = fraud signal) |

## 7. Derived Features (~30 features)

Computed inline in the SPCS scoring container using base velocity features from the Online Feature Store. These capture ratios and behavioural patterns.

### Time-of-day (4 features)
| Feature | Description |
|---------|-------------|
| `hour_of_day` | Hour (0-23) |
| `day_of_week` | Day (0=Mon, 6=Sun) |
| `is_weekend` | Saturday or Sunday |
| `is_night` | Between midnight and 5am |

### Amount deviation (2 features)
| Feature | Description |
|---------|-------------|
| `amount_pct_deviation` | % deviation from customer's historical average |
| `amount_ratio_to_hist_max` | Current amount / historical max |

### Decline rates (5 features)
| Feature | Description |
|---------|-------------|
| `decline_rate_{1hr,6hr,24hr,48hr,1wk}` | Declined / total attempts per window |

### Behavioural flags (5 features)
| Feature | Description |
|---------|-------------|
| `is_new_merchant` | Customer hasn't transacted here in past week |
| `is_international` | Merchant country is not GBR |
| `is_first_purchase` | Zero prior purchases |
| `is_new_account_7d` | Account registered within 7 days |
| `is_new_account_30d` | Account registered within 30 days |

### Customer history ratios (2 features)
| Feature | Description |
|---------|-------------|
| `pct_nongbr_purchases` | Proportion of historical purchases that were international |
| `dpan_per_purchase_ratio` | Distinct DPANs / total purchases |

### Velocity burst ratios (6 features)
| Feature | Description |
|---------|-------------|
| `customer_velocity_ratio_{1hr,6hr,24hr}_l1wk` | Short-window count / 1-week count |
| `customer_spend_burst_{1hr,6hr,24hr}_l1wk` | Short-window spend / 1-week spend |

### Merchant concentration (5 features)
| Feature | Description |
|---------|-------------|
| `merchant_concentration_{1hr,6hr,24hr,48hr,1wk}` | Same-merchant purchases / total per window |

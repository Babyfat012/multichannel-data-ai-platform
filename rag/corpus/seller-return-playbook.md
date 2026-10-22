<!-- Simulated internal playbook of the FSDS seller. -->
# Seller Return Playbook (internal, simulated)

## What the return label means
An order line is labeled positive when it is returned or refunded within 30 days after delivery.
A refund that only covers shipping does not count. The label is unknown until 30 days have passed.

## Reading the features
- `return_or_refund_rate_90d`: share of labeled lines of the SKU in the last 90 days that were
  positive. It is empty (null) when no line of the SKU has a mature label yet.
- `labeled_count_90d`: how many lines have a mature label; below 20 the rate is not reliable.
- `approved_returns_24h`: approved returns in the last 24 hours, from the live stream feed.

## Thresholds
- Rate above 15% with at least 20 labeled lines: review the listing (size chart, photos, description).
- Rate above 30%: stop ads for the SKU and contact the supplier about quality.
- More than 5 approved returns in 24 hours: check for a listing or packing mistake today.

## High predicted return risk on a new order
When the model predicts a high return probability for a new order line, the seller can add a size
reminder in the order message, double-check the packed item, or offer an exchange instead of a refund.

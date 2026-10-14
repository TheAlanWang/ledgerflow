-- Precision / recall of the streaming rules against PaySim's isFraud label.
-- Denominator for recall = all fraud transactions in the batch layer (daily_summary).
WITH total AS (SELECT sum(fraud_count) AS fraud FROM daily_summary),
per_rule AS (
    SELECT rule, count(DISTINCT txn_id) AS alerts, count(DISTINCT txn_id) FILTER (WHERE is_fraud = 1) AS tp
    FROM alerts GROUP BY rule
    UNION ALL
    SELECT 'ANY_RULE', count(DISTINCT txn_id), count(DISTINCT txn_id) FILTER (WHERE is_fraud = 1) FROM alerts
)
SELECT rule, alerts, tp,
       round(tp::numeric / nullif(alerts, 0), 3)  AS precision,
       round(tp::numeric / (SELECT fraud FROM total), 3) AS recall
FROM per_rule ORDER BY rule;

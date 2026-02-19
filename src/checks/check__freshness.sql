-- check: freshness
-- severity: warn
-- description: Most recent ingested_at in staging must be within 48 hours
SELECT
    latest_ingested_at,
    NOW() - latest_ingested_at AS staleness,
    INTERVAL '48 hours' AS threshold
FROM (
    SELECT MAX(ingested_at) AS latest_ingested_at
    FROM stg_transactions
)
WHERE latest_ingested_at < NOW() - INTERVAL '48 hours';

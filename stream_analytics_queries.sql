-- =====================================================================
-- Azure Stream Analytics – Marketing Analytics Queries
-- =====================================================================
--
-- Business Questions:
--   1. "Which device types are most active?"  → DeviceBreakdown
--   2. "Are there traffic spikes?"            → SpikeDetection
--
-- Architecture:
--   The clickstream Event Hub has two consumer groups:
--     $Default   – used by Stream Analytics (this job)
--     dashboard  – used by the Flask consumer (raw events for the live feed)
--
-- Input:  Event Hub "clickstream" (alias: clickstream-input, consumer group: $Default)
-- Output: Blob Storage            (alias: blob-output)
--
-- Both CTEs share a common schema so they can UNION into one output.
-- The query_type column identifies each row's origin.
-- =====================================================================

WITH DeviceBreakdown AS (
    SELECT
        'device_breakdown'                                          AS query_type,
        device                                                      AS dimension,
        COUNT(*)                                                    AS event_count,
        COUNT(DISTINCT user_id)                                     AS unique_users,
        SUM(CASE WHEN event_type = 'purchase'      THEN 1 ELSE 0 END) AS purchases,
        SUM(CASE WHEN event_type = 'add_to_cart'   THEN 1 ELSE 0 END) AS add_to_carts,
        SUM(CASE WHEN event_type = 'product_click' THEN 1 ELSE 0 END) AS product_clicks,
        SUM(CASE WHEN event_type = 'page_view'     THEN 1 ELSE 0 END) AS page_views,
        CAST(NULL AS nvarchar(max))                                 AS traffic_status,
        System.Timestamp()                                          AS window_end
    FROM
        [clickstream-input] TIMESTAMP BY timestamp
    GROUP BY
        device,
        TumblingWindow(minute, 1)
),
SpikeDetection AS (
    SELECT
        'spike_detection'                                           AS query_type,
        'all'                                                       AS dimension,
        COUNT(*)                                                    AS event_count,
        COUNT(DISTINCT user_id)                                     AS unique_users,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END)   AS purchases,
        CAST(NULL AS bigint)                                        AS add_to_carts,
        CAST(NULL AS bigint)                                        AS product_clicks,
        CAST(NULL AS bigint)                                        AS page_views,
        CASE
            WHEN COUNT(*) >= 150 THEN 'critical'
            WHEN COUNT(*) >= 100 THEN 'spike'
            ELSE 'normal'
        END                                                         AS traffic_status,
        System.Timestamp()                                          AS window_end
    FROM
        [clickstream-input] TIMESTAMP BY timestamp
    GROUP BY
        HoppingWindow(second, 60, 10)
)

SELECT * INTO [analytics-results-output] FROM DeviceBreakdown
UNION
SELECT * FROM SpikeDetection

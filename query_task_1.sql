

WITH ordered_actions AS (
  SELECT 
    id,
    id_user,
    action,
    timestamp_action,
    LAG(action) OVER (PARTITION BY id_user ORDER BY timestamp_action) as prev_action, 
    LEAD(action) OVER (PARTITION BY id_user ORDER BY timestamp_action) as next_action,
    ROW_NUMBER() OVER (PARTITION BY id_user ORDER BY timestamp_action) as action_sequence
  FROM user_sessions.sessions
  ORDER BY id_user, timestamp_action
)
SELECT 
  'Consecutive Same Actions' as issue_type,
  id_user,
  action,
  timestamp_action,
  prev_action,
  next_action,
  action_sequence
FROM ordered_actions
WHERE action = prev_action
   OR action = next_action
ORDER BY id_user, timestamp_action
LIMIT 50;

SELECT 
  id_user,
  SUM(CASE WHEN action = 'open' THEN 1 ELSE 0 END) as open_count,
  SUM(CASE WHEN action = 'close' THEN 1 ELSE 0 END) as close_count,
  SUM(CASE WHEN action = 'open' THEN 1 ELSE 0 END) - 
  SUM(CASE WHEN action = 'close' THEN 1 ELSE 0 END) as open_close_diff,
  MIN(timestamp_action) as first_action,
  MAX(timestamp_action) as last_action
FROM user_sessions.sessions
GROUP BY id_user
HAVING open_count != close_count
ORDER BY ABS(open_count - close_count) DESC
LIMIT 20;

----------------------------------------------------------------------------------------------------------------------------------
CREATE TEMP TABLE max_ts AS
SELECT
  MAX(timestamp_action) AS current_ts
FROM
  user_sessions.sessions;

CREATE TEMP TABLE window_range AS
SELECT
  current_ts,
  TIMESTAMP_SUB(current_ts, INTERVAL 9 DAY) AS window_start_ts
FROM
  max_ts;

-- Step 2: Flag true session opens (ignore consecutive “open”s)
CREATE TEMP TABLE ordered_events AS
SELECT
  id_user,
  action,
  timestamp_action,
  CASE
    WHEN action = 'open'
      AND LAG(action) OVER (PARTITION BY id_user ORDER BY timestamp_action) != 'open'
    THEN 1
    ELSE 0
  END AS is_open_start
FROM
  user_sessions.sessions;

-- Step 3: Number sessions per user
CREATE TEMP TABLE numbered_sessions AS
SELECT
  *,
  SUM(is_open_start) OVER (PARTITION BY id_user ORDER BY timestamp_action) AS session_num
FROM
  ordered_events;

-- Step 4: Extract each session’s open timestamp
CREATE TEMP TABLE opens AS
SELECT
  id_user,
  session_num,
  MIN(timestamp_action) AS open_ts
FROM
  numbered_sessions
WHERE
  is_open_start = 1
GROUP BY
  id_user, session_num;

-- Step 5: Extract each session’s close timestamp
CREATE TEMP TABLE closes AS
SELECT
  id_user,
  session_num,
  MIN(timestamp_action) AS close_ts
FROM
  numbered_sessions
WHERE
  action = 'close'
  AND session_num > 0
GROUP BY
  id_user, session_num;

-- Step 6: Build “clipped” sessions (clip opens before window, cap closes at now)
CREATE TEMP TABLE sessions AS
SELECT
  o.id_user,
  GREATEST(o.open_ts, w.window_start_ts) AS adj_open_ts,
  LEAST(IFNULL(c.close_ts, w.current_ts), w.current_ts) AS adj_close_ts
FROM
  opens AS o
LEFT JOIN
  closes AS c
ON
  o.id_user = c.id_user
  AND o.session_num = c.session_num
CROSS JOIN
  window_range AS w;

-- Step 7: Explode sessions into one row per calendar date they touch
CREATE TEMP TABLE exploded AS
SELECT
  id_user,
  adj_open_ts,
  adj_close_ts,
  session_date
FROM
  sessions,
  UNNEST(
    GENERATE_DATE_ARRAY(
      DATE(adj_open_ts),
      DATE(adj_close_ts)
    )
  ) AS session_date;

-- Step 8: Compute hours overlapped on each day
CREATE TEMP TABLE per_day AS
SELECT
  id_user,
  session_date,
  CASE
    WHEN LEAST(adj_close_ts, TIMESTAMP_ADD(TIMESTAMP(session_date), INTERVAL 1 DAY))
         > GREATEST(adj_open_ts, TIMESTAMP(session_date))
    THEN
      TIMESTAMP_DIFF(
        LEAST(adj_close_ts, TIMESTAMP_ADD(TIMESTAMP(session_date), INTERVAL 1 DAY)),
        GREATEST(adj_open_ts, TIMESTAMP(session_date)),
        SECOND
      )/3600.0
    ELSE 0
  END AS hours_in_day
FROM
  exploded;

-- Step 9: Aggregate per user per day for the last 10 calendar dates
SELECT
  id_user,
  session_date,
  SUM(hours_in_day) AS total_hours
FROM
  per_day
WHERE
  session_date BETWEEN
    DATE_SUB((SELECT DATE(current_ts) FROM max_ts), INTERVAL 9 DAY)
    AND (SELECT DATE(current_ts) FROM max_ts)
GROUP BY
  id_user, session_date
ORDER BY
  id_user, session_date;
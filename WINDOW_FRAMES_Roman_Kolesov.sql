--Task Description
--You need to construct a query that meets the following requirements:
--Generate a sales report for the 49th, 50th, and 51st weeks of 1999.
--Include a column named CUM_SUM to display the amounts accumulated during each week.
--Include a column named CENTERED_3_DAY_AVG to show the average sales for the previous, current, and following days using a centered moving average.
--For Monday, calculate the average sales based on the weekend sales (Saturday and Sunday) as well as Monday and Tuesday.
--For Friday, calculate the average sales on Thursday, Friday, and the weekend.
--Ensure that your calculations are accurate for the beginning of week 49 and the end of week 51.

-- CTE for aggregating daily sales data
WITH SalesData AS (
    SELECT
        s.time_id,
        t.day_name,
        t.calendar_week_number,
        -- Sum of daily sales
        SUM(s.amount_sold) AS daily_sales
    FROM
        sh.sales s
    JOIN
        sh.times t ON s.time_id = t.time_id
    WHERE
        t.calendar_year = 1999 AND
        -- Focusing on the 49th, 50th, and 51st weeks
        t.calendar_week_number IN (49, 50, 51)
    GROUP BY
        s.time_id, t.day_name, t.calendar_week_number
),

-- CTE for calculating cumulative sales
CumulativeSales AS (
    SELECT
        time_id,
        day_name,
        calendar_week_number,
        daily_sales,
        -- Calculating the cumulative sum for each week
        SUM(daily_sales) OVER (
            PARTITION BY calendar_week_number
            ORDER BY time_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS CUM_SUM
    FROM
        SalesData
),

-- CTE for calculating centered 3-day average with adjusted window frames
CenteredAvg AS (
    SELECT
        *,
        -- Adjusted centered 3-day average sales calculation
        CASE
            WHEN day_name = 'Monday' AND calendar_week_number = 49 THEN AVG(daily_sales) OVER (
                -- For the first Monday, average sales based only on Monday and Tuesday
                ORDER BY time_id
                ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
            )
            WHEN day_name = 'Friday' AND calendar_week_number = 51 THEN AVG(daily_sales) OVER (
                -- For the last Friday, average sales based only on Thursday and Friday
                ORDER BY time_id
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            )
            WHEN day_name = 'Monday' THEN AVG(daily_sales) OVER (
                -- For other Mondays, average sales based on Saturday, Sunday, Monday, and Tuesday
                ORDER BY time_id
                ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING
            )
            WHEN day_name = 'Friday' THEN AVG(daily_sales) OVER (
                -- For other Fridays, average sales based on Thursday, Friday, and the weekend (Saturday and Sunday)
                ORDER BY time_id
                ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING
            )
            ELSE AVG(daily_sales) OVER (
                -- For other days, the centered average of the previous, current, and following days
                ORDER BY time_id
                ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            )
        END AS CENTERED_3_DAY_AVG
    FROM
        CumulativeSales
)

-- Final selection of required columns
SELECT
    time_id,
    day_name,
    calendar_week_number,
    daily_sales,
    CUM_SUM,
    CENTERED_3_DAY_AVG
FROM
    CenteredAvg
-- Ordering by time_id to maintain chronological order
ORDER BY
    time_id;
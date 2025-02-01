USE mban_db;

-- View the tables
SELECT * FROM dimensions.customer_dimension;
SELECT * FROM dimensions.date_dimension;
SELECT * FROM dimensions.product_dimension;
SELECT * FROM fact_tables.conversions;
SELECT * FROM fact_tables.orders;

-- Customer 360 Table
WITH CustomerConversionData AS (
    -- Extract static customer and conversion data
    SELECT
        cd.customer_id,
        cd.first_name,
        cd.last_name,
        cv.conversion_id,
        ROW_NUMBER() OVER (PARTITION BY cd.customer_id ORDER BY cv.conversion_date) AS conversion_number,
        cv.conversion_type,
        cv.conversion_date,
        dd.year_week AS conversion_week,
        -- Define next conversion week to properly segment conversion periods
        LEAD(dd.year_week) OVER (PARTITION BY cd.customer_id ORDER BY cv.conversion_date) AS next_conversion_week,
        cv.conversion_channel
    FROM fact_tables.conversions cv
    JOIN dimensions.customer_dimension cd ON cv.fk_customer = cd.sk_customer
    JOIN dimensions.date_dimension dd ON cv.fk_conversion_date = dd.sk_date
),

FirstOrderPlaced AS (
    -- Identify the first order per customer
    SELECT
        customer_id,
        conversion_id,
        first_order_week,
        first_order_total_paid,
        first_order_product
    FROM (
        SELECT
            cd.customer_id,
            cv.conversion_id,
            dd.year_week AS first_order_week,
            o.price_paid AS first_order_total_paid,
            pd.product_name AS first_order_product,
            ROW_NUMBER() OVER (PARTITION BY cd.customer_id ORDER BY dd.year_week, o.order_number) AS row_num
        FROM fact_tables.orders o
        JOIN fact_tables.conversions cv ON o.order_number = cv.order_number
        JOIN dimensions.date_dimension dd ON o.fk_order_date = dd.sk_date
        JOIN dimensions.customer_dimension cd ON o.fk_customer = cd.sk_customer
        JOIN dimensions.product_dimension pd ON o.fk_product = pd.sk_product
    ) first_order
    WHERE row_num = 1
),

OrderHistory AS (
    -- Compile customer's order history with boolean order_placed column
    SELECT
        cd.customer_id,
        dd.year_week AS order_week,
        -- Convert to boolean: 1 if order exists, 0 otherwise
        CASE WHEN COUNT(o.order_id) > 0 THEN 1 ELSE 0 END AS order_placed,
        SUM(o.unit_price) AS total_before_discounts,
        SUM(o.discount_value) AS total_discounts,
        SUM(o.price_paid) AS total_paid_in_week
    FROM fact_tables.orders o
    JOIN dimensions.date_dimension dd ON o.fk_order_date = dd.sk_date
    JOIN dimensions.customer_dimension cd ON o.fk_customer = cd.sk_customer
    GROUP BY cd.customer_id, dd.year_week
),

AllWeeks AS (
    -- Generate all possible weeks for each customer within conversion periods
    SELECT DISTINCT cd.customer_id, dd.year_week
    FROM dimensions.date_dimension dd
    CROSS JOIN dimensions.customer_dimension cd
),

WeeklyData AS (
    -- Generate weekly data for each customer conversion
    SELECT
        cc.customer_id,
        cc.first_name,
        cc.last_name,
        cc.conversion_id,
        cc.conversion_number,
        cc.conversion_type,
        cc.conversion_date,
        cc.conversion_week,
        cc.next_conversion_week,
        cc.conversion_channel,
        fo.first_order_week,
        fo.first_order_total_paid,
        fo.first_order_product,
        aw.year_week AS order_week,
        -- Week counter for each conversion
        ROW_NUMBER() OVER (PARTITION BY cc.customer_id, cc.conversion_id ORDER BY aw.year_week) AS week_counter,
        -- Boolean: 1 if an order was placed, 0 otherwise
        COALESCE(oh.order_placed, 0) AS order_placed,
        COALESCE(oh.total_before_discounts, 0) AS total_before_discounts,
        COALESCE(oh.total_discounts, 0) AS total_discounts,
        COALESCE(oh.total_paid_in_week, 0) AS total_paid,
        -- Prevent revenue inflation by ensuring order_week falls within valid conversion periods
        SUM(COALESCE(oh.total_paid_in_week, 0)) 
            OVER (PARTITION BY cc.customer_id, cc.conversion_id 
                  ORDER BY aw.year_week
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS conversion_cumulative_revenue,
        SUM(COALESCE(oh.total_paid_in_week, 0)) 
            OVER (PARTITION BY cc.customer_id 
                  ORDER BY aw.year_week
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lifetime_cumulative_revenue
    FROM CustomerConversionData cc
    LEFT JOIN FirstOrderPlaced fo ON cc.customer_id = fo.customer_id AND cc.conversion_id = fo.conversion_id
    LEFT JOIN AllWeeks aw ON cc.customer_id = aw.customer_id 
        AND aw.year_week BETWEEN cc.conversion_week AND COALESCE(cc.next_conversion_week, aw.year_week)
    LEFT JOIN OrderHistory oh ON cc.customer_id = oh.customer_id AND aw.year_week = oh.order_week
    WHERE aw.year_week < cc.next_conversion_week OR cc.next_conversion_week IS NULL
)

-- Select from the final WeeklyData CTE
SELECT *
FROM WeeklyData
ORDER BY customer_id, conversion_number, week_counter;

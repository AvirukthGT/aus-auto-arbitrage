WITH raw_data AS (
    SELECT * FROM {{ source('raw_auto', 'raw_vehicle_listings') }}
),

cleaned AS (
    SELECT
        -- Generate surrogate primary key
        {{ dbt_utils.generate_surrogate_key(['Brand', 'Model', 'Year', 'Kilometres', 'Location', 'Price']) }} AS listing_id,

        -- Standardize categoricals and handle missing values
        NULLIF(TRIM(Brand), '-') AS brand,
        NULLIF(TRIM(Model), '-') AS model,
        CAST(Year AS INT64) AS vehicle_year,
        NULLIF(TRIM(Transmission), '-') AS transmission,
        NULLIF(TRIM(DriveType), '-') AS drive_type,
        NULLIF(TRIM(FuelType), '-') AS fuel_type,

        -- Cast core metrics
        CAST(REGEXP_REPLACE(Price, r'[^\d]', '') AS INT64) AS price,
        CAST(REGEXP_REPLACE(Kilometres, r'[^\d]', '') AS INT64) AS kilometres,

        -- Parse engine and fuel data via regex; handles '-' as NULL
        CAST(REGEXP_EXTRACT(Engine, r'(\d+)\s*cyl') AS INT64) AS engine_cylinders,
        CAST(REGEXP_EXTRACT(FuelConsumption, r'([0-9\.]+)\s*L') AS FLOAT64) AS fuel_consumption_l_100km,

        -- Split geography into city and state
        TRIM(SPLIT(Location, ',')[SAFE_OFFSET(0)]) AS city,
        TRIM(SPLIT(Location, ',')[SAFE_OFFSET(1)]) AS state

    FROM raw_data
    -- Strict filters based on EDA
    WHERE Price IS NOT NULL 
      AND Price != 'POA' 
      AND Kilometres IS NOT NULL
      AND Kilometres != '-'
      AND Location IS NOT NULL
      AND Location != '-'
)

SELECT * FROM cleaned
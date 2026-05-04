WITH raw_data AS (
    SELECT * FROM {{ source('raw_auto', 'raw_vehicle_listings') }}
),

cleaned AS (
    SELECT
        -- Generate surrogate primary key
        {{ dbt_utils.generate_surrogate_key(['Brand', 'Model', 'Year', 'Kilometres', 'Location', 'Price']) }} AS listing_id,

        -- Pass-through original categorical columns
        NULLIF(TRIM(Brand), '-') AS brand,
        CAST(Year AS INT64) AS vehicle_year,
        NULLIF(TRIM(Model), '-') AS model,
        
        -- Note: Python ingestion script converted 'Car/Suv' to 'Car_Suv'
        NULLIF(TRIM(Car_Suv), '-') AS car_suv,         
        NULLIF(TRIM(Title), '-') AS title,
        NULLIF(TRIM(UsedOrNew), '-') AS used_or_new,
        NULLIF(TRIM(Transmission), '-') AS transmission,
        NULLIF(TRIM(DriveType), '-') AS drive_type,
        NULLIF(TRIM(FuelType), '-') AS fuel_type,
        NULLIF(TRIM(BodyType), '-') AS body_type,
        
        -- Retain raw strings for downstream ML processing
        NULLIF(TRIM(Engine), '-') AS engine_raw,       
        NULLIF(TRIM(Location), '-') AS location_raw,
        NULLIF(TRIM(ColourExtInt), '-') AS colour_ext_int_raw,

        -- Cast core target metrics
        CAST(REGEXP_REPLACE(Price, r'[^\d]', '') AS INT64) AS price,
        CAST(REGEXP_REPLACE(Kilometres, r'[^\d]', '') AS INT64) AS kilometres,

        -- Extract numeric values from engineering and spec columns
        CAST(REGEXP_EXTRACT(CylindersinEngine, r'(\d+)') AS INT64) AS engine_cylinders, 
        CAST(REGEXP_EXTRACT(FuelConsumption, r'([0-9\.]+)') AS FLOAT64) AS fuel_consumption_l_100km,
        CAST(REGEXP_EXTRACT(Doors, r'(\d+)') AS INT64) AS doors,
        CAST(REGEXP_EXTRACT(Seats, r'(\d+)') AS INT64) AS seats,

        -- Extract features from composite columns
        TRIM(SPLIT(Location, ',')[SAFE_OFFSET(0)]) AS city,
        TRIM(SPLIT(Location, ',')[SAFE_OFFSET(1)]) AS state,
        TRIM(SPLIT(ColourExtInt, '/')[SAFE_OFFSET(0)]) AS exterior_colour

    FROM raw_data
    WHERE Price IS NOT NULL 
      AND Price != 'POA' 
      AND Kilometres IS NOT NULL
      AND Kilometres != '-'
      AND Location IS NOT NULL
      AND Location != '-'
)

SELECT * FROM cleaned
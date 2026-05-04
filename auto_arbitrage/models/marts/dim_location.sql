WITH staging AS (
    SELECT * FROM {{ ref('stg_vehicle_listings') }}
),

unique_locations AS (
    SELECT DISTINCT
        location_raw,
        city,
        state
    FROM staging
    WHERE location_raw IS NOT NULL
)

SELECT
    -- Surrogate key generated from raw location string
    {{ dbt_utils.generate_surrogate_key(['location_raw']) }} AS location_id,
    location_raw,
    city,
    state
FROM unique_locations
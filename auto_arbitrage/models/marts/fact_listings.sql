WITH staging AS (
    SELECT * FROM {{ ref('stg_vehicle_listings') }}
)

SELECT
    -- Primary key
    listing_id,

    -- Foreign keys referencing dimension surrogate keys
    {{ dbt_utils.generate_surrogate_key([
        'brand', 'model', 'vehicle_year', 'body_type', 'transmission', 'drive_type', 'fuel_type'
    ]) }} AS vehicle_id,
    
    {{ dbt_utils.generate_surrogate_key(['location_raw']) }} AS location_id,

    -- Degenerate dimensions (listing-specific data)
    title,
    used_or_new,
    exterior_colour,
    engine_raw,

    -- Core numerical metrics
    price,
    kilometres

FROM staging
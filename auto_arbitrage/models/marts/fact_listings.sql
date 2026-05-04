WITH staging AS (
    SELECT * FROM {{ ref('stg_vehicle_listings') }}
)

SELECT
    -- Primary key
    listing_id,

    -- Foreign keys referencing dimension surrogate keys
    -- Must perfectly match the dimension hash to join properly
    {{ dbt_utils.generate_surrogate_key([
        'brand', 'model', 'vehicle_year', 'body_type', 'car_suv', 
        'transmission', 'drive_type', 'fuel_type', 'engine_cylinders', 
        'fuel_consumption_l_100km', 'doors', 'seats'
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
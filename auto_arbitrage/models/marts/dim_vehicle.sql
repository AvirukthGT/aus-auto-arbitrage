WITH staging AS (
    SELECT * FROM {{ ref('stg_vehicle_listings') }}
),

unique_vehicles AS (
    SELECT DISTINCT
        brand,
        model,
        vehicle_year,
        body_type,
        car_suv,
        transmission,
        drive_type,
        fuel_type,
        engine_cylinders,
        fuel_consumption_l_100km,
        doors,
        seats
    FROM staging
    WHERE brand IS NOT NULL AND model IS NOT NULL
)

SELECT
    -- Generate surrogate key from vehicle spec combination
    -- Hash every single column that defines the vehicle's unique DNA
    {{ dbt_utils.generate_surrogate_key([
        'brand', 'model', 'vehicle_year', 'body_type', 'car_suv', 
        'transmission', 'drive_type', 'fuel_type', 'engine_cylinders', 
        'fuel_consumption_l_100km', 'doors', 'seats'
    ]) }} AS vehicle_id,
    *
FROM unique_vehicles
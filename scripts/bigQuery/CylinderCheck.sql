SELECT 
  Engine,
  REGEXP_EXTRACT(Engine, r'(\d+)\s*cyl') AS extracted_cylinders,
  FuelType
FROM `aus_auto_market.raw_vehicle_listings`
WHERE REGEXP_EXTRACT(Engine, r'(\d+)\s*cyl') IS NULL
  AND Engine IS NOT NULL
LIMIT 20;
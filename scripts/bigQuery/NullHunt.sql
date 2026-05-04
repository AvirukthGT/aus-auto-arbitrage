SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN Price IS NULL OR TRIM(Price) = '' THEN 1 ELSE 0 END) as missing_price,
  SUM(CASE WHEN Kilometres IS NULL OR TRIM(Kilometres) = '' OR Kilometres = '-' THEN 1 ELSE 0 END) as missing_kms,
  SUM(CASE WHEN Engine IS NULL OR TRIM(Engine) = '' THEN 1 ELSE 0 END) as missing_engine,
  SUM(CASE WHEN Location IS NULL OR TRIM(Location) = '' THEN 1 ELSE 0 END) as missing_location
FROM `aus_auto_market.raw_vehicle_listings`;
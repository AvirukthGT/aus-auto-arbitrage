SELECT 
  Price, 
  COUNT(*) as frequency
FROM `aus_auto_market.raw_vehicle_listings`
WHERE REGEXP_CONTAINS(Price, r'[^\d]') 
GROUP BY Price
ORDER BY frequency DESC;
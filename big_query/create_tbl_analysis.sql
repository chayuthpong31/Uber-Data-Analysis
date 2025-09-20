CREATE OR REPLACE TABLE `refined-area-472614-n2.uber_data_engineering_project.tbl_analytics` AS (
SELECT 
f.trip_id,
f.VendorID,
dt.tpep_pickup_datetime,
dt.tpep_dropoff_datetime,
pc.passenger_count,
t.trip_distance,
r.rate_code_name,
pu.Zone as pick_zone,
pu.Borough as pick_borough,
pu.service_zone as pick_service_zone,
d.Zone as drop_zone,
d.Borough as drop_borough,
d.service_zone as drop_service_zone,
pm.payment_type_name,
f.fare_amount,
f.extra,
f.mta_tax,
f.tip_amount,
f.tolls_amount,
f.improvement_surcharge,
f.total_amount
FROM refined-area-472614-n2.uber_data_engineering_project.fact_table f
JOIN refined-area-472614-n2.uber_data_engineering_project.datetime_dim dt ON f.datetime_id = dt.datetime_id
JOIN refined-area-472614-n2.uber_data_engineering_project.passenger_count_dim pc ON f.passenger_count_id = pc.passenger_count_id
JOIN refined-area-472614-n2.uber_data_engineering_project.trip_distance_dim t ON f.trip_distance_id = t.trip_distance_id
JOIN refined-area-472614-n2.uber_data_engineering_project.rate_code_dim r ON f.rate_code_id = r.rate_code_id
JOIN refined-area-472614-n2.uber_data_engineering_project.pickup_location_dim pu ON f.pick_location_id = pu.pick_location_id
JOIN refined-area-472614-n2.uber_data_engineering_project.dropoff_location_dim d ON f.drop_location_id = d.drop_location_id
JOIN refined-area-472614-n2.uber_data_engineering_project.payment_type_dim pm ON f.payment_type_id = pm.payment_type_id)
;
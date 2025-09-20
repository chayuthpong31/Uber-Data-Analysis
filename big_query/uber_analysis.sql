-- Find the total fare amount by vendorid
select  VendorID, SUM(fare_amount) as total_fare_amount from `refined-area-472614-n2.uber_data_engineering_project.fact_table` 
GROUP BY VendorID;

-- Find the average tip amount by payment type
SELECT p.payment_type_name, AVG(f.tip_amount) as average_tip_amount FROM `refined-area-472614-n2.uber_data_engineering_project.fact_table` f
JOIN refined-area-472614-n2.uber_data_engineering_project.payment_type_dim p
ON f.payment_type_id = p.payment_type_id
GROUP BY p.payment_type_name;

-- Find the top 10 pickuplocations based on the number of trips
SELECT p.Zone, COUNT(*) as number_of_trips FROM `refined-area-472614-n2.uber_data_engineering_project.fact_table` f
JOIN `refined-area-472614-n2.uber_data_engineering_project.pickup_location_dim` p
ON f.pick_location_id = p.pick_location_id
GROUP BY p.Zone
ORDER BY number_of_trips DESC
LIMIT 10;


-- Find the total number of trips by passenger count
SELECT p.passenger_count, COUNT(*) as number_of_trips FROM `refined-area-472614-n2.uber_data_engineering_project.fact_table` f
JOIN `refined-area-472614-n2.uber_data_engineering_project.passenger_count_dim` p
ON f.passenger_count_id = p.passenger_count_id
GROUP BY p.passenger_count
ORDER BY p.passenger_count

-- Find the average fare amount by hour of the day
SELECT d.drop_hour, AVG(f.fare_amount) as average_fare_amount FROM `refined-area-472614-n2.uber_data_engineering_project.fact_table` f
JOIN `refined-area-472614-n2.uber_data_engineering_project.datetime_dim` d
ON f.datetime_id = d.datetime_id
GROUP BY d.drop_hour
ORDER BY d.drop_hour


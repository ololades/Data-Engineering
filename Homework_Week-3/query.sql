
---create external table 
CREATE OR REPLACE EXTERNAL TABLE `green_taxi2022.green_externaltab`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://data-engr-zoomcamp_terabuck/green_tripdata_2022.parquet']
);


select * from `green_taxi2022.green_trip`
Limit 20


select * from `green_taxi2022.green_externaltab`
Limit 20

---Question 1: What is count of records for the 2022 Green Taxi Data??
select count(*) from `green_taxi2022.green_trip`



---Question 2:
---Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
---What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
select count(distinct(PULocationID)) from `green_taxi2022.green_trip`

select count(distinct(PULocationID)) from `green_taxi2022.green_externaltab`

-- Query for External Table
SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs,
FROM green_taxi2022.green_externaltab;

-- Query for Regular Table
SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs,
FROM green_taxi2022.green_trip;


-- Question 3:
-- How many records have a fare_amount of 0?
SELECT COUNT(fare_amount) AS amount,
FROM green_taxi2022.green_trip
where fare_amount = 0



-- Question 4:
-- What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

CREATE TABLE `green_taxi2022.optimized_table`
PARTITION BY `PUlocationID` AS
SELECT *
FROM `green_taxi2022.green_trip`
CLUSTER BY `lpep_pickup_datetime` WITHIN PARTITION(`PUlocationID`);


CREATE OR REPLACE TABLE `green_taxi2022.optimized_table`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * REPLACE(
  CAST(0 AS NUMERIC) AS VendorID,
  CAST(0 AS NUMERIC) AS payment_type
) FROM `green_taxi2022.green_externaltab`;




-- Question 5:
-- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

-- Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

SELECT distinct(PULocationID) as PULocationID
FROM `green_taxi2022.green_trip`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' and '2022-06-30';


SELECT distinct(PULocationID) as PULocationID
FROM `green_taxi2022.optimized_table`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' and '2022-06-30';
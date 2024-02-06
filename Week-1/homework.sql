---------------------------Question 2. Understanding docker first run
---Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed ( use pip list ).
docker run -it --entrypoint=bash python:3.9





----------------------------Question 3. Count records
----How many taxi trips were totally made on September 18th 2019? (Tip: started and finished on 2019-09-18).
----------------Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns are 
----------------in the format timestamp (date and hour+min+sec) and not in date (this has been transformed while ingesting the data).

SELECT COUNT(*) AS total_trips FROM ny_taxi
WHERE DATE(lpep_pickup_datetime) = '2019-09-18';


------------------------Question 4. Largest trip for each day
------Which was the pick up day with the largest trip distance Use the pick up time for your calculations.
SELECT lpep_pickup_datetime, trip_distance FROM ny_taxi
ORDER BY trip_distance desc
LIMIT 5


---------------------Question 5. Three biggest pick up Boroughs
---Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown
---Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

'''SELECT 
    DISTINCT nt.lpep_pickup_datetime AS pickup_datetime, 
    nt."PULocationID",
    SUM(nt.total_amount) AS total_amount,
	SUM(nt.fare_amount) AS fare_amount,
    tz."Borough",
    tz."Zone",
    tz."LocationID"
FROM 
    ny_taxi AS nt
LEFT JOIN 
    trips_zones AS tz ON nt."PULocationID" = tz."LocationID"
WHERE 
	 DATE(nt.lpep_pickup_datetime) = '2019-09-18' AND tz."Borough" != 'Unknown'
GROUP BY 
    nt.lpep_pickup_datetime, nt."PULocationID", tz."Borough", tz."Zone", tz."LocationID"
ORDER BY 
    fare_amount DESC
LIMIT 5;'''


SELECT 
    tz."Borough",
    SUM(nt.total_amount) AS total_amount
FROM 
    ny_taxi AS nt
LEFT JOIN 
    trips_zones AS tz ON nt."PULocationID" = tz."LocationID"
WHERE 
    DATE(nt.lpep_pickup_datetime) = '2019-09-18' AND tz."Borough" != 'unknown'
GROUP BY 
    tz."Borough"
HAVING 
    SUM(nt.total_amount) > 50000
ORDER BY 
    total_amount DESC
LIMIT 3;



-----------------------Question 6. Largest tip
---For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id.
SELECT 
    tz_drop."Zone" AS drop_off_zone,
    MAX(nt.tip_amount) AS max_tip_amount
FROM 
    ny_taxi AS nt
JOIN 
    trips_zones AS tz_pick ON nt."PULocationID" = tz_pick."LocationID"
JOIN 
    trips_zones AS tz_drop ON nt."DOLocationID" = tz_drop."LocationID"
WHERE 
    DATE(nt.lpep_pickup_datetime) >= '2019-09-01' AND DATE(nt.lpep_pickup_datetime) <= '2019-09-30'
    AND tz_pick."Zone" = 'Astoria'
GROUP BY 
    tz_drop."Zone"
ORDER BY 
    max_tip_amount DESC
LIMIT 5;





---------check a table info
SELECT *
FROM information_schema.columns
WHERE table_name = 'ny_taxi' AND column_name = 'PULocationID';



-----------------------Question 6: prepare the environment by creating resources in GCP with Terraform.
------------------------------ create a GCP Bucket and Big Query Dataset.
-- 1: Install Terraform on your local machine. 

-- 2: Set Up GCP Credentials: Go to gcp, create a project
        --(Make sure you have GCP credentials configured on your machine). 
        -----You can set this up by creating a service account key in the GCP Console 
        -----and then exporting the JSON key file. Set the GOOGLE_APPLICATION_CREDENTIALS 
        -----environment variable to point to this key file. 
        --(export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/keyfile.json") (echo $ GOOGLE_APPLICATION_CREDENTIALS)

-- 3: Create a Terraform Configuration File:
        --Create a new Terraform configuration file with a .tf extension. 
        --Let's call it main.tf. This file will contain the infrastructure code to create GCP resources.

-- 4: Create Variables File:
        --Create a separate variables file, e.g., variables.tfvars, to store your sensitive or configurable values.

-- 5: Initialize Terraform:
        --Open a terminal in the directory where your Terraform configuration file (main.tf) is located and run: 
        --(terraform init)
        
        --terraform fmt (to organise code)
        --terraform plan 
        --terraform apply
Data Engineering Zoomcamp homework 1:

Answers:

1- On Gitbash: docker build --help

--iidfile string Write the image ID to the file

2- Answer: 3 packages

#pip list
Package Version

---

pip 22.0.4
setuptools 58.1.0
wheel 0.38.4

3- Answer: 20530

select COUNT(1)
from
taxi_data_ny t
where
DATE(lpep_pickup_datetime)='2019-01-15' AND
DATE(lpep_dropoff_datetime) ='2019-01-15'

4- Answer: 2019-01-15

select
lpep_pickup_datetime,
lpep_dropoff_datetime,
trip_distance
from
taxi_data_ny order by trip_distance desc

5- Answer: 2:1282 - 3:254

select COUNT(1)
from
taxi_data_ny
where
DATE(lpep_pickup_datetime)='2019-01-01' and  
passenger_count=2

select COUNT(1)
from
taxi_data_ny
where
DATE(lpep_pickup_datetime)='2019-01-01' and  
passenger_count=3

6- Answer: Long Island City/Queens Plaza

select

lpep_pickup_datetime,
lpep_dropoff_datetime,
tip_amount,
zpu."zone" AS "pick_up_loc",
zdo."zone" AS "dropoff_loc"

from
taxi_data_ny t JOIN ny_zones zpu
ON t."PULocationID" = zpu."locationid"
JOIN ny_zones zdo ON t."DOLocationID"=zdo."locationid"
where zpu."zone" = 'Astoria'
order by tip_amount desc
Limit 1

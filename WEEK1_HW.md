QUESTION 1
```python
docker build --help 
```

QUESTION 2
Running an image:

```python
winpty docker run -it --entrypoint=bash python:3.9 
```

Listing python packages/modules installed:

```python
pip list 
```

QUESTION 3. COUNT RECORDS
```python
SELECT COUNT(*)
FROM green_taxi_data
WHERE  to_char(lpep_pickup_datetime, 'YYYY-MM-DD')='2019-01-15' 
        AND to_char(lpep_dropoff_datetime, 'YYYY-MM-DD')='2019-01-15';
```

QUESTION 4. Day with the largest trip distance

```python
SELECT lpep_pickup_datetime
FROM green_taxi_data
WHERE trip_distance = (SELECt MAX(trip_distance) 
                        FROM green_taxi_data); 
```


QUESTION 5. The number of passengers

```python
SELECT passenger_count, COUNT(passenger_count)
FROM green_taxi_data
WHERE passenger_count IN (2,3) 
		AND (to_char(lpep_pickup_datetime, 'YYYY-MM-DD'))='2019-01-01'
GROUP BY passenger_count;

```

QUESTION 6. Largest tip


```python
SELECT "t"."Zone"
FROM taxi_zone_data t
WHERE "t"."LocationID" = (SELECT "g"."DOLocationID"
						  FROM green_taxi_data g
						  JOIN taxi_zone_data t
						  ON "g"."PULocationID" = "t"."LocationID"
						  WHERE "t"."Zone" = 'Astoria'
						  GROUP BY "g"."DOLocationID"
						  ORDER BY MAX(tip_amount) DESC
						  LIMIT 1);
```
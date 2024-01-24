-- Part 1: Design the DW tables

-- TASK 1: Design the dimension table MyDimDate
/*
The company is looking at a granularity of day, which means they would like
to have the ability to generate the report on yearly, monthly, daily, and
weekday basis.
*/

-- SOLUTION, Task 1
/*
Table: MyDimDate
date
year
date_id
quarter_num
month_num
month_name
day_of_month_num
day_of_week_num
day_name
*/

-- TASK 2: Design the dimension table MyDimWaste
-- SOLUTION, Task 2
/*
Table: MyDimWaste
waste_id
waste_type
*/

-- TASK 3: Design the dimension table MyDimZone
-- SOLUTION, Task 3
/*
Table: MyDimZone
zone_id
zone_name
city
*/

-- TASK 4: Design the fact table MyFactTrips
-- SOLUTION, Task 4
/*
Table: MyFactTrips
trip_id
trip_num
waste_id
waste_collected
zone_id
date_id
*/

-- Part 2: Crete the DW tables on PostgreSQL

-- TASK 5: Create the dimension table MyDimDate
-- SOLUTION, Task 5
CREATE TABLE IF NOT EXISTS public."MyDimDate" (
	date_id 	integer NOT NULL,
	date 		date 	NOT NULL,
	year		integer,
	quarter_num	integer,
	month_num	integer,
	month_name	character varying (20),
	day_of_month_num	integer,
	day_of_week_num		integer,
	day_name	character varying (20),
	PRIMARY KEY (date_id)
);

-- TASK 6: Create the dimension table MyDimWaste
-- SOLUTION, Task 6
CREATE TABLE IF NOT EXISTS public."MyDimWaste" (
	waste_id integer NOT NULL,
	waste_type character varying (20) NOT NULL,
	PRIMARY KEY (waste_id)
);

-- TASK 7: Create the dimension table MyDimZone
-- Solution, Task 7
CREATE TABLE IF NOT EXISTS public."MyDimZone" (
	zone_id integer NOT NULL,
	zone_name character varying (20),
	city character varying (20),
	PRIMARY KEY (zone_id)
);

-- TASK 8: Create the fact table MyFactTrips
-- SOLUTION, Task 8
CREATE TABLE IF NOT EXISTS public."MyFactTrips" (
	trip_id		integer NOT NULL,
	trip_num	integer NOT NULL,
	waste_id	integer,
	waste_collected double precision,
	zone_id 	integer,
	date_id		integer,
	PRIMARY KEY (trip_id),
	CONSTRAINT MyDimWaste_FK FOREIGN KEY (waste_id) REFERENCES public."MyDimWaste" (waste_id),
	CONSTRAINT MyDimZone_FK FOREIGN KEY (zone_id) REFERENCES public."MyDimZone" (zone_id),
	CONSTRAINT MyDimDate_FK FOREIGN KEY (date_id) REFERENCES public."MyDimDate" (date_id)
);


-- Part 3: Load data into the DW tables
/*
After the initial schema design, you were told that due to operations issues,
data could not be collected in the format initially planned. This implies that
the previous tables (MyDimDate, MyDimWaste, MyDimZone, MyFactTrips) and their
associated attributes are no longer applicable to the current design. the
company has loaded data using CSV files per the new design.
*/

-- Links to CSV files:
/*
Data for DimDate table: https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0260EN-SkillsNetwork/labs/Final%20Assignment/DimDate.csv
Data for DimTruck table: https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0260EN-SkillsNetwork/labs/Final%20Assignment/DimTruck.csv
Data for DimStation table: https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0260EN-SkillsNetwork/labs/Final%20Assignment/DimStation.csv
Data for FactTrips table: https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0260EN-SkillsNetwork/labs/Final%20Assignment/FactTrips.csv
*/

-- Created the new DW tables as per revised schema prior to data loading.

-- Table: DimDate
create table if not exists public."DimDate" (
	dateid		integer not null,
	date		date	not null,
	year		integer,
	quarter		integer,
	quartername	character (2),
	month		integer,
	monthname	character varying (15),
	day			integer,
	weekday		integer,
	weekdayname	character varying (15),
	primary key (dateid)
);

-- Table: DimTruck
create table if not exists public."DimTruck" (
	truckid		integer not null,
	trucktype	character varying (20) not null,
	primary key (truckid)
);

-- Table: DimStation
create table if not exists public."DimStation" (
	stationid	integer not null,
	city		character varying (20) not null,
	primary key (stationid)
);

-- Table: FactTrips
create table if not exists public."FactTrips" (
	tripid		integer not null,
	dateid		integer,
	stationid	integer,
	truckid	integer,
	wastecollected double precision,
	primary key (tripid),
	constraint DimDate_fk foreign key (dateid) references public."DimDate" (dateid),
	constraint DimStation_fk foreign key (stationid) references public."DimStation" (stationid),
	constraint DimTruck_fk foreign key (truckid) references public."DimTruck" (truckid)
);

-- TASKS 10 to 12: Load the data from the above CSV files into the correspondin tables.
-- I used pgAdmin's Data Import/Export utility to load the data from CSV files into tables.

-- Part 4: Write aggregation queries and create MQT's

-- TASK 13: Create a GROUPTING SET query
select stationid, trucktype, sum(wastecollected) as totalwaste
from public."FactTrips"
left join public."DimTruck" on public."DimTruck".truckid = public."FactTrips".truckid
group by grouping sets (stationid, trucktype)
order by stationid, trucktype;

-- TASK 14: Create a ROLLUP query
select year, city, ft.stationid, sum(wastecollected) as totalwaste
from public."FactTrips" as ft
left join public."DimDate" as dd on dd.dateid = ft.dateid
left join public."DimStation" as ds on ds.stationid = ft.stationid
group by rollup (year, city, ft.stationid)
order by year, city, ft.stationid;

-- TASK 15: Create a CUBE query
select year, city, ft.stationid, avg(wastecollected) as averagewaste
from public."FactTrips" as ft
left join public."DimDate" as dd on dd.dateid = ft.dateid
left join public."DimStation" as ds on ds.stationid = ft.stationid
group by cube (year, city, ft.stationid)
order by year, city, ft.stationid;

-- TASK 16: Create an MQT (Materialized Query Table)
-- Solution, Task 16
create materialized view if not exists public."Max_Waste_Stats" as (
	select city, ft.stationid, trucktype, max(wastecollected) as maxwaste
	from public."FactTrips" as ft
	join public."DimStation" as ds on ds.stationid = ft.stationid
	join public."DimTruck" as dt on dt.truckid = ft.truckid
	group by city, ft.stationid, trucktype
	order by city, ft.stationid, trucktype
);

-- Retrive data from the MQT
select * from public."Max_Waste_Stats";
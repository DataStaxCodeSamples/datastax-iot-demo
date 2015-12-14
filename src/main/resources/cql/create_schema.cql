create keyspace if not exists datastax_iot_demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};

use datastax_iot_demo; 

create table if not exists smart_meter (
	meter_id int,
	date timestamp,
	readings double, 
	status varchar, 
	units varchar,
	primary key (meter_id, date)
) WITH CLUSTERING ORDER BY(date desc); 

create table if not exists smart_meter_reading (
	meter_id int,
	date timestamp,
	source_id text,
	readings text,
	PRIMARY KEY(meter_id, date)
) WITH CLUSTERING ORDER BY(date desc); 

create table if not exists smart_meter_reading_aggregates (
	meter_id int,
	date timestamp,
	aggregatetype text,
	source_id text,
	readings double,
	PRIMARY KEY((meter_id, aggregatetype), date)
) WITH CLUSTERING ORDER BY(date desc); 
	
create table if not exists billing_cycle(
	billing_cycle int,
	meter_id int,
	PRIMARY KEY (billing_cycle, meter_id)
);	
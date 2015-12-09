create keyspace if not exists datastax_iot_demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};

use datastax_iot_demo; 

create table if not exists smart_meter (
	meter_id text,
	date timestamp,
	readings text, 
	status varchar, 
	units varchar,
	primary key (meter_id, date)
); 

create table if not exists smart_meter_reading (
	meter_id text,
	date timestamp,
	source_id text,
	readings text,
	PRIMARY KEY(meter_id, date)
);

create table if not exists smart_meter_reading_aggregates (
	meter_id text,
	date timestamp,
	aggregatetype text,
	source_id text,
	readings text,
	PRIMARY KEY((meter_id, aggregatetype), date)
);
	
	


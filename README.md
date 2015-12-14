# datastax-iot-demo

This is a small demo to show how to insert meter readings for a smart reader. Note : the readings come in through a file with
a no of readings per day. 

## Schema Setup
Note : This will drop the keyspace "datastax_iot_demo" and create a new one. All existing data will be lost. 

To specify contact points use the contactPoints command line parameter e.g. 

	'-DcontactPoints=192.168.25.100,192.168.25.101'
	
The contact points can take mulitple points in the IP,IP,IP (no spaces).

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

To create the schema, run the following

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup" -DcontactPoints=localhost
		
	
To insert some meter readings, run the following 
	
	mvn clean compile exec:java -Dexec.mainClass="com.datastax.smartmeter.Main" -DcontactPoints=localhost
	
You can use -DnoOfCustomers and -DnoOfDays to change the no of customer readings and the no of days (in the past) to be inserted. Defaults are 100 and 180 respectively.

To view the data using cqlsh, run

	select * from smart_meter_reading where meter_id = 1;
	
To run a billingCycle, which accummulates usages for a specific time period, run	

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.smartmeter.BillingCycleProcessor" -DcontactPoints=localhost

To specific billing cycle use -DbillingCycle (Default is 7).

To run an DAY aggregation, which sums the usage for a day, run	

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.smartmeter.Aggregate" -DcontactPoints=localhost

You can use -DnoOfCustomers and -DnoOfDays to change the no of customer readings and the no of days (in the past) to be aggregated. Defaults are 100 and 180 respectively.

To view the data using cqlsh, run

	select * from smart_meter_reading_aggregates where meter_id = 1 and aggregatetype ='DAY';

To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown" -DcontactPoints=localhost
    


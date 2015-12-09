package com.datastax.tickdata;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.tickdata.model.SmartMeter;
import com.datastax.tickdata.model.SmartMeterReading;
import com.datastax.tickdata.model.SmartMeterReadingFile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SmartMeterReadingDao {
	
	private static Logger logger = LoggerFactory.getLogger(SmartMeterReadingDao.class);

	

	private Session session;
	private static String keyspaceName = "datastax_iot_demo";
	private static String smartMeter = keyspaceName + ".smart_meter";
	private static String smartMeterReading = keyspaceName + ".smart_meter_reading";
	private static String smartMeterReadingAgg = keyspaceName + ".smart_meter_reading_aggregates";

	private static final String INSERT_INTO_METER = "Insert into " + smartMeter + " (meter_id, date, readings, status, units) values (?,?,?,?,?);";
	private static final String INSERT_INTO_READINGS = "Insert into " + smartMeterReading + " (meter_id, date, source_id, readings) values (?,?,?,?);";
	private static final String INSERT_INTO_READINGS_AGG = "Insert into " + smartMeterReadingAgg + " (meter_id, date, aggregatetype, source_id, readings) values (?,?,?,?,?);";
	
	private PreparedStatement insertStmtMeter;
	private PreparedStatement insertStmtReading;
	private PreparedStatement insertStmtReadingAgg;
	private ObjectMapper jsonMapper = new ObjectMapper();
	private TypeReference<HashMap<Integer,Double>> typeRef = new TypeReference<HashMap<Integer,Double>>() {};
	
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.zzz"); 

	public SmartMeterReadingDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder()
				.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
				.addContactPoints(contactPoints).build();
		
		this.session = cluster.connect();

		this.insertStmtMeter = session.prepare(INSERT_INTO_METER);		
		this.insertStmtMeter.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
		this.insertStmtReading = session.prepare(INSERT_INTO_READINGS);		
		this.insertStmtReading.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
		this.insertStmtReadingAgg = session.prepare(INSERT_INTO_READINGS_AGG);		
		this.insertStmtReadingAgg.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
	}

	public void insertMeterReadings(SmartMeterReadingFile file){
		
		SmartMeterReading reading = file.getSmartMeterReading();
		SmartMeter meter = file.getSmartMeter();
		
		BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
		
		BoundStatement bs = new BoundStatement(insertStmtReading);
		batch.add(bs.bind(reading.getId(), reading.getDate(), reading.getSourceId(), getJsonReadings(reading)));
		bs = new BoundStatement(insertStmtMeter);
		batch.add(bs.bind(meter.getMeterId(), meter.getReadDate(), meter.getReadings(), meter.getStatus(), meter.getUnits()));

		session.execute(batch);
	}

	private String getJsonReadings(SmartMeterReading reading) {
		try {
			return jsonMapper.writeValueAsString(reading.getReadings());
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			throw new RuntimeException (e.getMessage() + " - Readings cannot be turned into JSON - " + reading );
		}
	}

	private Map<Integer,Double> readJsonReadings(String readings) {
		try {
			return jsonMapper.readValue(readings, typeRef); 
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException (e.getMessage() + " - JSON Readings cannot be turned into map - " + readings );
		}
	}
}

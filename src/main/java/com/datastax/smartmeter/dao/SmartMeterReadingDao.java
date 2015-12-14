package com.datastax.smartmeter.dao;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.colt.list.IntArrayList;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.smartmeter.model.BillingCycle;
import com.datastax.smartmeter.model.SmartMeter;
import com.datastax.smartmeter.model.SmartMeterReading;
import com.datastax.smartmeter.model.SmartMeterReadingAgg;
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
	private static String billingCycle = keyspaceName + ".billing_cycle";

	private static final String INSERT_INTO_BC = "Insert into " + billingCycle + " (billing_cycle, meter_id) values (?,?);";
	private static final String INSERT_INTO_METER = "Insert into " + smartMeter + " (meter_id, date, readings, status, units) values (?,?,?,?,?);";
	private static final String INSERT_INTO_READINGS = "Insert into " + smartMeterReading + " (meter_id, date, source_id, readings) values (?,?,?,?);";
	private static final String INSERT_INTO_READINGS_AGG = "Insert into " + smartMeterReadingAgg + " (meter_id, date, aggregatetype, source_id, readings) values (?,?,?,?,?);";

	private static final String SELECT_FROM_BC = "select billing_cycle, meter_id from " + billingCycle + " where billing_cycle = ?";
	private static final String SELECT_FROM_METER = "select meter_id, date, readings, status, units from " + smartMeter + " where meter_id = ? limit 1";
	private static final String SELECT_FROM_READINGS = "select meter_id, date, source_id, readings from " + smartMeterReading + " where meter_id = ? limit ?";
	private static final String SELECT_FROM_READINGS_BY_DATE = "select meter_id, date, source_id, readings from " + smartMeterReading + " where meter_id = ? and date > ? and date <= ?";
	private static final String SELECT_FROM_READINGS_AGG = "select meter_id, aggregatetype, date, source_id, readings from " + smartMeterReadingAgg + " where meter_id = ? and aggregatetype = ? limit ?";		
	
	private PreparedStatement insertStmtBillingCylce;
	private PreparedStatement insertStmtMeter;
	private PreparedStatement insertStmtReading;
	private PreparedStatement insertStmtReadingAgg;
	private PreparedStatement selectStmtBC;
	private PreparedStatement selectStmtMeter;
	private PreparedStatement selectStmtReading;
	private PreparedStatement selectStmtReadingByDate;
	private PreparedStatement selectStmtReadingAgg;
	
	private ObjectMapper jsonMapper = new ObjectMapper();
	private TypeReference<HashMap<Integer,Double>> typeRef = new TypeReference<HashMap<Integer,Double>>() {};
	
	private SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.zzz"); 

	public SmartMeterReadingDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder()
				.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
				.addContactPoints(contactPoints).build();
		
		this.session = cluster.connect();

		this.insertStmtBillingCylce = session.prepare(INSERT_INTO_BC);
		this.insertStmtMeter = session.prepare(INSERT_INTO_METER);		
		this.insertStmtReading = session.prepare(INSERT_INTO_READINGS);		
		this.insertStmtReadingAgg = session.prepare(INSERT_INTO_READINGS_AGG);		
		
		this.selectStmtBC = session.prepare(SELECT_FROM_BC);
		this.selectStmtMeter = session.prepare(SELECT_FROM_METER);		
		this.selectStmtReading = session.prepare(SELECT_FROM_READINGS);		
		this.selectStmtReadingByDate = session.prepare(SELECT_FROM_READINGS_BY_DATE);		
		this.selectStmtReadingAgg = session.prepare(SELECT_FROM_READINGS_AGG);		
	}

	public SmartMeter selectSmartMeter(int meterNo){
		BoundStatement bs = new BoundStatement(selectStmtMeter);
		ResultSet resultSet = session.execute(bs.bind(meterNo));
		
		Row row = resultSet.one();
		if (row==null){
			throw new RuntimeException("Smart meter " + meterNo + " not available.");
		}else{
			return new SmartMeter(row.getInt("meter_id"), row.getTimestamp("date"), row.getDouble("readings"), 
					row.getString("status"), row.getString("units"));
		}			
	}
	
	public List<SmartMeterReading> selectSmartMeterReadings(int meterNo){
		return this.selectSmartMeterReadings(meterNo, 10000);
	}

	public List<SmartMeterReading> selectSmartMeterReadings(int meterNo, int days){
		BoundStatement bs = new BoundStatement(selectStmtReading);
		ResultSet resultSet = session.execute(bs.bind(meterNo, days));
		
		List <SmartMeterReading> readings = new ArrayList<>();
		
		List<Row> rows = resultSet.all();
		if (rows==null){
			throw new RuntimeException("Smart meter " + meterNo + " not available.");
		}else{
			
			for (Row row : rows){
				readings.add(new SmartMeterReading(row.getInt("meter_id"), row.getTimestamp("date"),  
					row.getString("source_id"), readJsonReadings(row.getString("readings"))));
			}
			
			return readings;
		}
	}
	
	public List<SmartMeterReading> selectSmartMeterReadings(int meterNo, Date from, Date to){
		
		BoundStatement bs = new BoundStatement(selectStmtReadingByDate);
		ResultSet resultSet = session.execute(bs.bind(meterNo, from, to));
		List <SmartMeterReading> readings = new ArrayList<>();
		
		List<Row> rows = resultSet.all();
		if (rows==null){
			throw new RuntimeException("Smart meter " + meterNo + " not available.");
		}else{
			
			for (Row row : rows){
				readings.add(new SmartMeterReading(row.getInt("meter_id"), row.getTimestamp("date"),  
					row.getString("source_id"), readJsonReadings(row.getString("readings"))));
			}
			
			return readings;
		}
	}
	
	public IntArrayList selectMeterNosForBillingCycle(int billingCycle, 
			BlockingQueue<List<SmartMeterReading>> queueMeterReadings, Date from, Date to){
		
		IntArrayList meterNos = new IntArrayList();
		
		BoundStatement bs = new BoundStatement(this.selectStmtBC);
		ResultSet results = session.execute(bs.bind(billingCycle));
		
		List<Row> all = results.all();
		for (Row row:all){
			
			queueMeterReadings.add(this.selectSmartMeterReadings(row.getInt("meter_id"), from, to));
		}
		
		return meterNos;
	}
	
	public IntArrayList selectMeterNosForBillingCycle(int billingCycle){
		
		IntArrayList meterNos = new IntArrayList();
		
		BoundStatement bs = new BoundStatement(this.selectStmtBC);
		ResultSet results = session.execute(bs.bind(billingCycle));
		
		List<Row> all = results.all();
		for (Row row:all){
			
			meterNos.add(row.getInt("meter_id"));
		}
		
		return meterNos;
	}

	public void insertBillingCycle(BillingCycle bc){
		
		BoundStatement bs = new BoundStatement(this.insertStmtBillingCylce);
		session.execute(bs.bind(bc.getBillingCycle(), bc.getMeterNo()));
	}
	
	public void insertMeterDetails(SmartMeter meter){
		
		BoundStatement bs = new BoundStatement(insertStmtMeter);
		session.execute(bs.bind(meter.getMeterId(), meter.getReadDate(), meter.getReadings(), meter.getStatus(), meter.getUnits()));
	}

	public void insertMeterReadings(SmartMeterReading reading){
		
		BoundStatement bs = new BoundStatement(insertStmtReading);
		session.execute(bs.bind(reading.getId(), reading.getDate(), reading.getSourceId(), getJsonReadings(reading)));
	}

	public void insertMeterReadingsAgg(SmartMeterReadingAgg readingAggregate){
		
		BoundStatement bs = new BoundStatement(insertStmtReadingAgg);
		session.execute(bs.bind(readingAggregate.getId(), readingAggregate.getDate(), readingAggregate.getAggregatetype(), 
				readingAggregate.getSourceId(), readingAggregate.getValue()));
	}

	public void insertMeterReadingsAgg(List<SmartMeterReadingAgg> readingAggregates){
		
		BoundStatement bs;
		
		AsyncWriterWrapper wrapper = new AsyncWriterWrapper();
		
		for (SmartMeterReadingAgg readingAggregate : readingAggregates){
			
			bs = new BoundStatement(insertStmtReadingAgg);
			wrapper.addStatement(bs.bind(readingAggregate.getId(), readingAggregate.getDate(), readingAggregate.getAggregatetype(), 
				readingAggregate.getSourceId(), readingAggregate.getValue()));
		}
		wrapper.executeAsync(session);
		logger.info("Processed : " + wrapper.getStatementCounter() + " statements");
	}

	
	public List<SmartMeterReadingAgg> selectSmartMeterReadingsAgg(int meterNo, String aggregateType){
		return this.selectSmartMeterReadingsAgg(meterNo, aggregateType, 10000);
	}
	
	public List<SmartMeterReadingAgg> selectSmartMeterReadingsAgg(int meterNo, String aggregateType, int limit){
		BoundStatement bs = new BoundStatement(selectStmtReadingAgg);
		ResultSet resultSet = session.execute(bs.bind(meterNo, aggregateType, limit));
		
		List <SmartMeterReadingAgg> readings = new ArrayList<>();
		
		List<Row> rows = resultSet.all();
		if (rows==null){
			throw new RuntimeException("Smart meter " + meterNo + " and aggregate type " + aggregateType + " not available.");
		}else{
			
			for (Row row : rows){
				readings.add(new SmartMeterReadingAgg(row.getInt("meter_id"), row.getString("aggregatetype"), row.getTimestamp("date"),  
					row.getString("source_id"), row.getDouble("readings")));
			}
			
			return readings;
		}
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

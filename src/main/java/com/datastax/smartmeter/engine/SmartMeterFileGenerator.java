package com.datastax.smartmeter.engine;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.smartmeter.model.SmartMeter;
import com.datastax.smartmeter.model.SmartMeterReading;
import com.datastax.smartmeter.model.SmartMeterReadingFile;

public class SmartMeterFileGenerator implements Iterator<SmartMeterReadingFile> {
	
	private static Logger logger = LoggerFactory.getLogger(SmartMeterFileGenerator.class);
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	
	private int noOfCustomers;
	private int meterNo = 0;
	private int noOfDays = 0;
	private DateTime startTime;
	private DateTime now;
	private DecimalFormat decimalFormatter = new DecimalFormat("#.#");
	
	private Map<Integer, Double> lastMeterReadingMap = new HashMap<Integer, Double>();

	public SmartMeterFileGenerator(int noOfCustomers, int noOfDays) {
		this.noOfCustomers = noOfCustomers;
		this.noOfDays = noOfDays;
		
		this.startTime = new DateTime().minusDays((noOfDays-1)).withMillisOfDay(0);
		this.now = DateTime.now();
	}

	@Override
	public boolean hasNext() {
		
		if (now.isBefore(this.startTime)){
			return false;
		}
		return true;
	}

	@Override
	public SmartMeterReadingFile next() {		
		
		Map<Integer, Double> readings = new HashMap<Integer, Double>();		
		double total = 0;
		double lastMeterReading = getLastMeterReading(meterNo);
		DateTime today = this.startTime;
		
		SmartMeter meter = new SmartMeter(meterNo, today.toDate(), lastMeterReading, "ON", "KW");
		
		for (int i=0; i < 48; i++){
			
			double startValue = this.createRandomValue();
			readings.put(today.getMinuteOfDay()/30, startValue);									
			total += startValue;
			
			today = today.plusMinutes(30);			
		}			
		
		lastMeterReadingMap.put(meterNo, lastMeterReading + total);
		
		SmartMeterReading reading = new SmartMeterReading(meterNo, this.startTime.toDate(), meterNo+"", readings);		
		SmartMeterReadingFile file = new SmartMeterReadingFile(meter, reading);
		meterNo++;
	
		//Reset meterNo
		if (meterNo+1 >= noOfCustomers){
			this.startTime = this.startTime.plusDays(1).withMillisOfDay(0);
			meterNo = 0; 
		}
		
		return file;
	}

	private double getLastMeterReading(int meterNo) {
		
		if (this.lastMeterReadingMap.get(meterNo) == null){
			return 0;
		}
		
		return lastMeterReadingMap.get(meterNo);
	}

	@Override
	public void remove() {
		throw new NotImplementedException();
	}

	private double createRandomValue() {

		return new Double(Math.random() * 10).intValue();
	}
	
	public static void main(String args[]){
		
		
		SmartMeterFileGenerator temp = new SmartMeterFileGenerator(10, 2);
		
		logger.info("Starting....");
		while (temp.hasNext()){
			SmartMeterReadingFile next = temp.next();
			logger.info(next.toString());
		}
		logger.info("Finished");
	}
}

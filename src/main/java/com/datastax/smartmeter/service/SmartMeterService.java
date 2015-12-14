package com.datastax.smartmeter.service;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.smartmeter.dao.SmartMeterReadingDao;
import com.datastax.smartmeter.model.SmartMeterReading;
import com.datastax.smartmeter.model.SmartMeterReadingAgg;

public class SmartMeterService {

	private static Logger logger = LoggerFactory.getLogger(SmartMeterService.class);
	
	private final SmartMeterReadingDao dao;
	
	public SmartMeterService(){
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		dao = new SmartMeterReadingDao(contactPointsStr.split(","));		
	}
	
	public List<SmartMeterReading> getReadingsLastNDays(int meterNo, int days){
		
		return this.dao.selectSmartMeterReadings(meterNo, days);
	}
	
	public List<SmartMeterReading> getReadingsByDate(int meterNo, Date from, Date to){
		
		return this.dao.selectSmartMeterReadings(meterNo, from, to);
	}

	public List<SmartMeterReadingAgg> getReadingAgg (int meterNo, String aggregateType){
		
		return this.dao.selectSmartMeterReadingsAgg(meterNo, aggregateType);
	}
	
	public static void main(String args[]){
		new SmartMeterService();
	}
}

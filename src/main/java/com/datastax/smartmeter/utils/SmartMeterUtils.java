package com.datastax.smartmeter.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.datastax.smartmeter.model.AggregateType;
import com.datastax.smartmeter.model.SmartMeterReading;
import com.datastax.smartmeter.model.SmartMeterReadingAgg;

public class SmartMeterUtils {
	
	public static List<SmartMeterReadingAgg> aggregatePerReading(AggregateType aggregateType, List<SmartMeterReading> readings){
		
		List<SmartMeterReadingAgg> aggregates = new ArrayList<>();
		
		for (SmartMeterReading reading : readings ){
			
			if (aggregateType.equals(AggregateType.DAY)){
				aggregates.add(new SmartMeterReadingAgg(reading.getId(), aggregateType.name(), reading.getDate(), reading.getSourceId(), 
						SmartMeterUtils.sum(reading)));
			}		
		}
		return aggregates;	
	}
	
	public static double sum (SmartMeterReading reading){
		
		double sum = 0;
		
		Map<Integer, Double> map = reading.getReadings();
		Iterator<Double> iterator = map.values().iterator();

		while(iterator.hasNext()){
			sum += iterator.next();
		}
		return sum; 
	}


	public static double avg (SmartMeterReading reading){
		
		double sum = sum(reading);
		int count = reading.getReadings().size();
		
		return sum/count; 
	}
	
	
	public static double sum (List<SmartMeterReading> readings){
		
		double sum = 0;
		
		for (SmartMeterReading reading : readings){
			
			Map<Integer, Double> map = reading.getReadings();
			Iterator<Double> iterator = map.values().iterator();

			while(iterator.hasNext()){
				sum += iterator.next();
			}
		}		
		
		return sum; 
	}

	public static double avg (List<SmartMeterReading> readings){
		
		double sum = 0;
		int count = 0;
		
		for (SmartMeterReading reading : readings){
			
			Map<Integer, Double> map = reading.getReadings();
			Iterator<Double> iterator = map.values().iterator();

			while(iterator.hasNext()){
				sum += iterator.next();
				count++;
			}
		}		
		
		return sum/count; 
	}
	
}

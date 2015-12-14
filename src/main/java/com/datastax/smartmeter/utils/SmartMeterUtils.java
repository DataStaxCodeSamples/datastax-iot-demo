package com.datastax.smartmeter.utils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.datastax.smartmeter.model.SmartMeterReading;

public class SmartMeterUtils {
	
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

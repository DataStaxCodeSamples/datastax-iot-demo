package com.datastax.smartmeter.model;

public class SmartMeterReadingFile {

	private SmartMeter smartMeter;	
	private SmartMeterReading smartMeterReading;
	
	public SmartMeterReadingFile(SmartMeter smartMeter, SmartMeterReading smartMeterReading) {
		super();
		this.smartMeter = smartMeter;
		this.smartMeterReading = smartMeterReading;
	}
	public SmartMeter getSmartMeter() {
		return smartMeter;
	}
	public SmartMeterReading getSmartMeterReading() {
		return smartMeterReading;
	}
	@Override
	public String toString() {
		return "SmartReadingFile [smartMeter=" + smartMeter.toString() + ", smartMeterReading=" + smartMeterReading.toString() + "]";
	}
}

package com.datastax.smartmeter.model;

import java.util.Date;
import java.util.Map;

public class SmartMeterReading {
	
	private int id;
	private Date date;
	private String sourceId;	
	private Map<Integer, Double> readings;

	public SmartMeterReading(int id, Date date, String sourceId, Map<Integer, Double> readings) {
		this.id = id;
		this.date = date;
		this.sourceId = sourceId;
		this.readings = readings;
	}

	public Date getDate() {
		return date;
	}

	public String getSourceId() {
		return sourceId;
	}

	public int getId() {
		return id;
	}

	public Map<Integer, Double> getReadings() {
		return readings;
	}

	public void setReadings(Map<Integer, Double> readings) {
		this.readings = readings;
	}

	@Override
	public String toString() {
		return "SmartMeterReading [id=" + id + ", date=" + date + ", sourceId=" + sourceId + ", readings=" + readings
				+ "]";
	}
}

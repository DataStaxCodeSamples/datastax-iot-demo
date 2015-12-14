package com.datastax.smartmeter.model;

import java.util.Date;

public class SmartMeter {	
	private int meterId;
	
	private Date readDate;
	private double readings;
	private String status;
	private String units;
	
	public SmartMeter(int meterId, Date readDate, double readings, String status, String units) {
		super();
		this.meterId = meterId;
		this.readDate = readDate;
		this.readings = readings;
		this.status = status;
		this.units = units;
	}
	
	public int getMeterId() {
		return meterId;
	}
	public Date getReadDate() {
		return readDate;
	}
	public double getReadings() {
		return readings;
	}
	public String getStatus() {
		return status;
	}
	public String getUnits() {
		return units;
	}
	@Override
	public String toString() {
		return "SmartMeter [meterId=" + meterId + ", readDate=" + readDate + ", readings=" + readings + ", status="
				+ status + ", units=" + units + "]";
	}
}

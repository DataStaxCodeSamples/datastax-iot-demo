package com.datastax.smartmeter.model;

import java.util.Date;
import java.util.Map;

public class SmartMeterReadingAgg {
	
	private int id;
	private String aggregatetype;
	private Date date;
	private String sourceId;	
	private Double value;

	public SmartMeterReadingAgg(int id, String aggregatetype, Date date, String sourceId, double value) {
		this.id = id;
		this.aggregatetype = aggregatetype;
		this.date = date;
		this.sourceId = sourceId;
		this.value = value;
	}

	public int getId() {
		return id;
	}

	public String getAggregatetype() {
		return aggregatetype;
	}

	public Date getDate() {
		return date;
	}

	public String getSourceId() {
		return sourceId;
	}

	public Double getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "SmartMeterReadingAgg [id=" + id + ", aggregatetype=" + aggregatetype + ", date=" + date + ", sourceId="
				+ sourceId + ", value=" + value + "]";
	}
}

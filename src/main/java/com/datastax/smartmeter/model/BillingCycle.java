package com.datastax.smartmeter.model;

public class BillingCycle {

	private int billingCycle;
	private int meterNo;
	
	public BillingCycle(int billingCycle, int meterNo) {
		super();
		this.billingCycle = billingCycle;
		this.meterNo = meterNo;
	}
	public int getBillingCycle() {
		return billingCycle;
	}
	public int getMeterNo() {
		return meterNo;
	}
	
	@Override
	public String toString() {
		return "BillingCycle [billingCycle=" + billingCycle + ", meterNo=" + meterNo + "]";
	}
}

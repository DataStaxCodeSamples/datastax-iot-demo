package com.datastax.smartmeter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.KillableRunner;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.ThreadUtils;
import com.datastax.demo.utils.Timer;
import com.datastax.smartmeter.dao.SmartMeterReadingDao;
import com.datastax.smartmeter.model.SmartMeterReading;
import com.datastax.smartmeter.utils.SmartMeterUtils;

public class BillingCycleProcessor {
	private static Logger logger = LoggerFactory.getLogger(BillingCycleProcessor.class);

	public BillingCycleProcessor() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String noOfThreadsStr = PropertyHelper.getProperty("noOfThreads", "5");
		int billingCycle = Integer.parseInt(PropertyHelper.getProperty("billingCycle", "7"));
		
		SmartMeterReadingDao dao = new SmartMeterReadingDao(contactPointsStr.split(","));		
		int noOfThreads = Integer.parseInt(noOfThreadsStr);
		
		//Create shared queue 
		BlockingQueue<List<SmartMeterReading>> queueMeterReadings = new ArrayBlockingQueue<List<SmartMeterReading>>(1000);
		List<KillableRunner> tasks = new ArrayList<>();
		
		//Executor for Threads
		ExecutorService executor = Executors.newFixedThreadPool(noOfThreads);
		Timer timer = new Timer();
		timer.start();
		
		for (int i = 0; i < noOfThreads; i++) {
			KillableRunner task = new SmartMeterReadingAggregator(dao, queueMeterReadings);
			executor.execute(task);
			tasks.add(task);
		}
		
		Date from = DateTime.now().withMillisOfDay(0).withDayOfMonth(billingCycle).minusMonths(1).toDate(); 
		Date to = DateTime.now().withMillisOfDay(0).withDayOfMonth(billingCycle).toDate();
		
		dao.selectMeterNosForBillingCycle(billingCycle, queueMeterReadings, from, to);
				
		ThreadUtils.shutdown(tasks, executor);
		System.exit(0);
	}
	
	class SmartMeterReadingAggregator implements KillableRunner {

		private volatile boolean shutdown = false;
		private SmartMeterReadingDao dao;
		private BlockingQueue<List<SmartMeterReading>> queue;

		public SmartMeterReadingAggregator(SmartMeterReadingDao dao, BlockingQueue<List<SmartMeterReading>> queue) {
			this.dao = dao;
			this.queue = queue;
		}

		@Override
		public void run() {
			List<SmartMeterReading> readings;
			while(!shutdown){				
				readings = queue.poll(); 
				
				if (readings!=null){
					try {
						double sum = SmartMeterUtils.sum(readings);
						
						logger.info ("Billing " + sum + " for meter-no : " + readings.get(0).getId()); 
					} catch (Exception e) {
						e.printStackTrace();
					}
				}				
			}				
		}
		
		@Override
		public void shutdown() {
	        shutdown = true;
	    }
	}

	private void sleep(int seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new BillingCycleProcessor();
	}
}

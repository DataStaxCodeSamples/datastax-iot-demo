package com.datastax.smartmeter;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.smartmeter.BillingCycleProcessor.SmartMeterReadingAggregator;
import com.datastax.smartmeter.dao.SmartMeterReadingDao;
import com.datastax.smartmeter.engine.SmartMeterFileGenerator;
import com.datastax.smartmeter.model.SmartMeterReading;
import com.datastax.smartmeter.model.SmartMeterReadingFile;

public class Aggregate {
	private static Logger logger = LoggerFactory.getLogger(Aggregate.class);

	public Aggregate() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String noOfThreadsStr = PropertyHelper.getProperty("noOfThreads", "5");
		
		Integer noOfCustomers = Integer.parseInt(PropertyHelper.getProperty("noOfCustomers", "100000"));
		Integer noOfDays= Integer.parseInt(PropertyHelper.getProperty("noOfDays", "7"));
		
		SmartMeterReadingDao dao = new SmartMeterReadingDao(contactPointsStr.split(","));		

		int noOfThreads = Integer.parseInt(noOfThreadsStr);
		
		//Create shared queue 
		BlockingQueue<List<SmartMeterReading>> queueMeterReadings = new ArrayBlockingQueue<List<SmartMeterReading>>(1000);		
		
		//Executor for Threads
		ExecutorService executor = Executors.newFixedThreadPool(noOfThreads);
		Timer timer = new Timer();
		timer.start();
		
//		for (int i = 0; i < noOfThreads; i++) {
//			executor.execute(new SmartMeterReadingAggregator(dao, queueMeterReadings));
//		}
								
		logger.info(dao.selectSmartMeter(1).toString());		
		logger.info(dao.selectSmartMeterReadings(1).toString());				
		logger.info(dao.selectSmartMeterReadings(1, DateTime.now().minusDays(10).toDate(), DateTime.now().toDate()).toString());				
		logger.info(dao.selectMeterNosForBillingCycle(3).toString());		
		
		while(!queueMeterReadings.isEmpty() ){
			sleep(1);
		}		
		
		timer.end();
		
		System.exit(0);
	}
	
	class SmartMeterReadingWriter implements Runnable {

		private SmartMeterReadingDao dao;
		private BlockingQueue<SmartMeterReadingFile> queue;

		public SmartMeterReadingWriter(SmartMeterReadingDao dao, BlockingQueue<SmartMeterReadingFile> queue) {
			this.dao = dao;
			this.queue = queue;
		}

		@Override
		public void run() {
			SmartMeterReadingFile smartMeterReadingFile;
			while(true){				
				smartMeterReadingFile = queue.poll(); 
				
				if (smartMeterReadingFile!=null){
					try {
						this.dao.insertMeterReadings(smartMeterReadingFile.getSmartMeterReading());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}				
			}				
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
		new Aggregate();
	}
}

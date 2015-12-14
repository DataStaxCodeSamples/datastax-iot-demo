package com.datastax.smartmeter;

import java.util.Arrays;
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
import com.datastax.smartmeter.dao.SmartMeterReadingDao;
import com.datastax.smartmeter.engine.SmartMeterFileGenerator;
import com.datastax.smartmeter.model.BillingCycle;
import com.datastax.smartmeter.model.SmartMeter;
import com.datastax.smartmeter.model.SmartMeterReadingFile;

public class Main {
	private static Logger logger = LoggerFactory.getLogger(Main.class);

	public Main() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String noOfThreadsStr = PropertyHelper.getProperty("noOfThreads", "5");
		
		Integer noOfCustomers = Integer.parseInt(PropertyHelper.getProperty("noOfCustomers", "100"));
		Integer noOfDays= Integer.parseInt(PropertyHelper.getProperty("noOfDays", "180"));
		
		SmartMeterReadingDao dao = new SmartMeterReadingDao(contactPointsStr.split(","));		

		int noOfThreads = Integer.parseInt(noOfThreadsStr);
		//Create shared queue 
		BlockingQueue<SmartMeterReadingFile> queue = new ArrayBlockingQueue<SmartMeterReadingFile>(1000);
		
		//Executor for Threads
		ExecutorService executor = Executors.newFixedThreadPool(noOfThreads);
		Timer timer = new Timer();
		timer.start();
		
		DateTime startTime = new DateTime().minusDays((noOfDays-1)).withMillisOfDay(0);
		for (int i =0; i < noOfCustomers; i ++){
			dao.insertMeterDetails(new SmartMeter(i, startTime.toDate(), new Double(Math.random() * 10000).intValue(), "ON", "KW"));
			
			int billingCycle = billingCycles.get(new Double(Math.random() * billingCycles.size()).intValue());
			dao.insertBillingCycle(new BillingCycle(billingCycle, i));
		}
			
		for (int i = 0; i < noOfThreads; i++) {
			executor.execute(new SmartMeterReadingWriter(dao, queue));
		}
				
		//Start the tick generator
		SmartMeterFileGenerator tickGenerator = new SmartMeterFileGenerator(noOfCustomers, noOfDays);
		
		while (tickGenerator.hasNext()){
			SmartMeterReadingFile next = tickGenerator.next();
			
			try {
				queue.put(next);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		while(!queue.isEmpty() ){
			sleep(1);
		}		
		
		timer.end();
		logger.info("Data Loading took " + timer.getTimeTakenSeconds() + " secs for " + noOfCustomers +" customers and " +noOfDays+ " days.");
		
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
	
	private List<Integer> billingCycles = Arrays.asList(1,7,15,23);
	
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
		new Main();
	}
}

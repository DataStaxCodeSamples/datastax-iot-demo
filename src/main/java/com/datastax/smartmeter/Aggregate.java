package com.datastax.smartmeter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.KillableRunner;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.ThreadUtils;
import com.datastax.demo.utils.Timer;
import com.datastax.smartmeter.dao.SmartMeterReadingDao;
import com.datastax.smartmeter.model.AggregateType;
import com.datastax.smartmeter.model.SmartMeterReading;
import com.datastax.smartmeter.model.SmartMeterReadingAgg;
import com.datastax.smartmeter.utils.SmartMeterUtils;

public class Aggregate {
	private static Logger logger = LoggerFactory.getLogger(Aggregate.class);

	public Aggregate() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String noOfThreadsStr = PropertyHelper.getProperty("noOfThreads", "5");
		
		Integer noOfCustomers = Integer.parseInt(PropertyHelper.getProperty("noOfCustomers", "100"));
		Integer noOfDays= Integer.parseInt(PropertyHelper.getProperty("noOfDays", "180"));
		
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
				
		//Aggregate by date
		for (int i=0; i < noOfCustomers; i ++){			
			queueMeterReadings.add(dao.selectSmartMeterReadings(i));
		}
								
//		logger.info(dao.selectSmartMeter(1).toString());		
//		logger.info(dao.selectSmartMeterReadings(1).toString());				
//		logger.info(dao.selectSmartMeterReadings(1, DateTime.now().minusDays(10).toDate(), DateTime.now().toDate()).toString());				
//		logger.info(dao.selectMeterNosForBillingCycle(3).toString());		
		
		timer.end();
		logger.info("Data Aggregation took " + timer.getTimeTakenSeconds() + " secs for " + noOfCustomers +" customers and " +noOfDays+ " days.");

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
						List<SmartMeterReadingAgg> aggregatePerReading = SmartMeterUtils.aggregatePerReading(AggregateType.DAY, readings);						
						this.dao.insertMeterReadingsAgg(aggregatePerReading);
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
		new Aggregate();
	}
}

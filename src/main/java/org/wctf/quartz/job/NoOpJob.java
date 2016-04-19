package org.wctf.quartz.job;

import java.util.Map.Entry;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DisallowConcurrentExecution
public class NoOpJob implements Job {
	private static final Logger log = LoggerFactory.getLogger(NoOpJob.class);

	public NoOpJob() {
	}

	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobDataMap datamap = context.getMergedJobDataMap();
		log.info("jobName is "+context.getJobDetail().getKey().getName());
		for (Entry<String, Object> entry : datamap.entrySet()) {
			log.info("JobDataMap key is "+entry.getKey()+", JobDataMap value is "+entry.getValue());
		}
		Integer i = (Integer) datamap.get("counter");
		i = (i == null) ? 10 : i;
		while (i > 0) {
			try {
				Thread.sleep(1000);
				log.info("jobName:" + context.getJobDetail().getKey().getName() + "======jobKey:" + context.getTrigger().getKey().getName() + "============" + i + "+++++" + System.currentTimeMillis());
				i--;
				datamap.put("counter", i);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
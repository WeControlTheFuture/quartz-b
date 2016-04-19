package org.wctf.quartz.listener;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.quartz.JobPersistenceException;
import org.quartz.SchedulerException;
import org.quartz.SchedulerListener;
import org.quartz.impl.jdbcjobstore.PersistMoreJobStoreSupport;
import org.quartz.listeners.SchedulerListenerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 处理调度异常监听的表
 * 
 * @author 毕希研
 * 
 */
public class ExceptionToDBSchedulerListener extends SchedulerListenerSupport implements SchedulerListener {
	private final Logger log = LoggerFactory.getLogger(getClass());
	private PersistMoreJobStoreSupport persistMoreJobStoreSupport;

	public ExceptionToDBSchedulerListener(PersistMoreJobStoreSupport persistMoreJobStoreSupport) {
		this.persistMoreJobStoreSupport = persistMoreJobStoreSupport;
	}

	@Override
	public void schedulerError(String msg, SchedulerException cause) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream(baos);
			cause.printStackTrace(ps);
			
			persistMoreJobStoreSupport.storeJobExecutionException(msg, new String(baos.toByteArray()));
		} catch (JobPersistenceException e) {
			log.error("", e);
		}
	}

}

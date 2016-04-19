package org.wctf.quartz;

import org.quartz.SchedulerFactory;
import org.quartz.impl.jdbcjobstore.PersistMoreJobStoreSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wctf.quartz.ex.StdSchedulerEx;
import org.wctf.quartz.ex.StdSchedulerFactoryEx;
import org.wctf.quartz.listener.ExceptionToDBSchedulerListener;


public class QuartzDaemon {
	private static final Logger log = LoggerFactory.getLogger(QuartzDaemon.class);
	/**
	 * 启动
	 */
	private final static String DAEMON_START = "start";
	/**
	 * 停止
	 */
	private final static String DAEMON_STOP = "stop";
	/**
	 * 暂停
	 */
	private final static String DAEMON_PAUSE = "pause";
	/**
	 * 继续
	 */
	private final static String DEAMON_RESUME = "resume";

	/**
	 * 调度实例id
	 */
	private final static String INSTANCEID = "org.quartz.scheduler.instanceId";

	public static void main(String[] args) {
		try {
			String flag = DAEMON_START;
			if (null != args && args.length > 0) {
				flag = args[0];
				System.setProperty(INSTANCEID, args[1]);
			}
			if (!flag.toLowerCase().equals(DAEMON_START) && !flag.toLowerCase().equals(DAEMON_STOP) && !flag.toLowerCase().equals(DAEMON_PAUSE) && !flag.toLowerCase().equals(DEAMON_RESUME)) {
				flag = DAEMON_START;
			}
			System.setProperty("org.quartz.properties", "quartz/quartz.properties");
			SchedulerFactory schedulerFactory = new StdSchedulerFactoryEx();
			StdSchedulerEx scheduler = (StdSchedulerEx) schedulerFactory.getScheduler();
			PersistMoreJobStoreSupport persistMoreJobStoreSupport = (PersistMoreJobStoreSupport) scheduler.getRsrcs().getJobStore();
			scheduler.getListenerManager().addSchedulerListener(new ExceptionToDBSchedulerListener(persistMoreJobStoreSupport));
			if (flag.equals(DAEMON_START)) {
				if (log.isInfoEnabled())
					log.info("Schedule Task Daemon Started!");
				scheduler.start();
			}
			if (flag.equals(DAEMON_STOP)) {
				if (log.isInfoEnabled())
					log.info("Schedule Task Daemon starting stop...., pls. wait!");
				scheduler.pauseAll();
				scheduler.shutdown(true);
				if (log.isInfoEnabled())
					log.info("Schedule Task Daemon stop over!");
			}
			if (flag.equals(DAEMON_PAUSE)) {
				if (log.isInfoEnabled())
					log.info("Schedule Task Daemon paused all!");
				scheduler.pauseAll();
			}
			if (flag.equals(DEAMON_RESUME)) {
				if (log.isInfoEnabled())
					log.info("Schedule Task Daemon resume all!");
				scheduler.resumeAll();
			}
		} catch (Exception ex) {
			log.error("", ex);
		}
	}
}

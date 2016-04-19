package org.wctf.quartz.ex.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.quartz.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobConfigWatcher implements Watcher {
	private final Logger log = LoggerFactory.getLogger(getClass());
	private Scheduler scheduler;
	private ZkXmlJobOper zkXmlJobOper;
	private String jobName;
	private ZooKeeper zk;

	public JobConfigWatcher() {
	}

	public JobConfigWatcher(Scheduler scheduler, ZkXmlJobOper zkXmlJobOper, String jobName, ZooKeeper zk) {
		this.scheduler = scheduler;
		this.zkXmlJobOper = zkXmlJobOper;
		this.jobName = jobName;
		this.zk = zk;
	}

	@Override
	public void process(WatchedEvent event) {
		try {
			log.info("call JobConfigWatcher process.............................");
			try {
				while (!scheduler.isStarted())
					Thread.sleep(1000);
			} catch (Exception e) {
				log.info("error sleep in process", e);
			}
			if (Event.EventType.NodeDataChanged.equals(event.getType())) {
				log.info("modify job " + jobName);
				byte[] b = zk.getData("/" + jobName, this, null);
				zkXmlJobOper.addAndModifyJob(jobName, new String(b, "GBK"));
			} else
				log.info("got " + event.getType() + " from zookeeper.......................but I can not deal with it");
		} catch (Exception e) {
			log.error("error process watcher ", e);
		}
	}

	public ZooKeeper getZk() {
		return zk;
	}

	public void setZk(ZooKeeper zk) {
		this.zk = zk;
	}

}

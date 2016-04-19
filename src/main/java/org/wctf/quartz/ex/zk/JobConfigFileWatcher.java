package org.wctf.quartz.ex.zk;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.quartz.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobConfigFileWatcher implements Watcher {
	private final Logger log = LoggerFactory.getLogger(getClass());
	private Scheduler scheduler;
	private ZooKeeper zk;
	private ZkXmlJobOper zkXmlJobOper;
	private List<String> children;

	public JobConfigFileWatcher() {
	}

	public JobConfigFileWatcher(Scheduler scheduler, ZkXmlJobOper zkXmlJobOper) {
		this.scheduler = scheduler;
		this.zkXmlJobOper = zkXmlJobOper;
	}

	public void process(WatchedEvent event) {
		try {
			log.info("call JobConfigFileWatcher process.............................");
			try {
				while (!scheduler.isStarted())
					Thread.sleep(1000);
			} catch (Exception e) {
				log.info("error sleep in process", e);
			}
			if (Event.EventType.NodeChildrenChanged.equals(event.getType())) {
				log.info("got NodeChildrenChanged from zookeeper.......................");
				List<String> list = zk.getChildren("/", this);
				if (list.size() > children.size()) {
					// add
					Set<String> set = new HashSet<>(list);
					set.removeAll(children);
					Iterator<String> it = set.iterator();
					while (it.hasNext()) {
						String name = it.next();
						log.info("add new job " + name);
						JobConfigWatcher jcw = new JobConfigWatcher(scheduler, zkXmlJobOper, name, zk);
						// jcw.setZk(zk);
						byte[] b = zk.getData("/" + name, jcw, null);
						zkXmlJobOper.addAndModifyJob(name, new String(b, "GBK"));
					}
				} else {
					// delete
					Set<String> set = new HashSet<>(children);
					set.removeAll(list);
					Iterator<String> it = set.iterator();
					while (it.hasNext()) {
						String name = it.next();
						log.info("remove job " + name);
						zkXmlJobOper.removeJob(name, name + "Group");
					}
				}

			} else {
				log.info("got " + event.getType() + " from zookeeper.......................but I can not deal with it");
			}
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

	public void setChildren(List<String> children) {
		this.children = children;
	}

}

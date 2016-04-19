package org.wctf.quartz.ex.zk;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.transaction.UserTransaction;

import org.apache.zookeeper.ZooKeeper;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.plugins.SchedulerPluginWithUserTransactionSupport;
import org.quartz.spi.ClassLoadHelper;

public class ZkXMLSchedulingDataProcessorPlugin extends SchedulerPluginWithUserTransactionSupport implements ZkXmlJobOper {
//	private final Logger log = LoggerFactory.getLogger(getClass());
	private static final String JOB_INITIALIZATION_PLUGIN_NAME = "ZkJobSchedulingDataLoaderPlugin";

	private boolean failOnFileNotFound = true;

	private String zkConnectString;
	private int zkTimeout;
	private ZooKeeper zk;

	// Populated by initialization
	private Map<String, String> jobFiles = new LinkedHashMap<String, String>();

	boolean started = false;

	protected ClassLoadHelper classLoadHelper = null;

	public String getZkConnectString() {
		return zkConnectString;
	}

	public void setZkConnectString(String zkConnectString) {
		this.zkConnectString = zkConnectString;
	}

	public int getZkTimeout() {
		return zkTimeout;
	}

	public void setZkTimeout(int zkTimeout) {
		this.zkTimeout = zkTimeout;
	}

	public boolean isFailOnFileNotFound() {
		return failOnFileNotFound;
	}

	public void setFailOnFileNotFound(boolean failOnFileNotFound) {
		this.failOnFileNotFound = failOnFileNotFound;
	}

	public void initialize(String name, final Scheduler scheduler, ClassLoadHelper schedulerFactoryClassLoadHelper) throws SchedulerException {
		super.initialize(name, scheduler);
		this.classLoadHelper = schedulerFactoryClassLoadHelper;
		JobConfigFileWatcher jcfw = new JobConfigFileWatcher(scheduler, this);

		try {
			zk = new ZooKeeper(zkConnectString, zkTimeout, jcfw);
			List<String> list = zk.getChildren("/", jcfw);
			for (String jobName : list) {
				JobConfigWatcher jcw = new JobConfigWatcher(scheduler, this, jobName, zk);
				// jcw.setZk(zk);
				byte[] b = zk.getData("/" + jobName, jcw, null);
				jobFiles.put(jobName, new String(b, "GBK"));
			}
			jcfw.setChildren(list);
		} catch (Exception e) {
			throw new SchedulerException(e);
		}
		jcfw.setZk(zk);
		getLog().info("Registering Quartz Job Initialization Plug-in.");
	}

	@Override
	public void start(UserTransaction userTransaction) {
		if (jobFiles.isEmpty() == false) {
			for (Entry<String, String> entry : jobFiles.entrySet()) {
				addAndModifyJob(entry.getKey(), entry.getValue());
			}
		}
		started = true;
	}

	/**
	 * Overriden to ignore <em>wrapInUserTransaction</em> because shutdown()
	 * does not interact with the <code>Scheduler</code>.
	 */
	@Override
	public void shutdown() {
		// Since we have nothing to do, override base shutdown so don't
		// get extranious UserTransactions.
	}

	public void addAndModifyJob(String jobName, String jobXmlContent) {
		if (jobName == null || jobXmlContent == null) {
			return;
		}

		try {
			XMLSchedulingDataProcessor2 processor = new XMLSchedulingDataProcessor2(this.classLoadHelper);

			processor.addJobGroupToNeverDelete(JOB_INITIALIZATION_PLUGIN_NAME);
			processor.addTriggerGroupToNeverDelete(JOB_INITIALIZATION_PLUGIN_NAME);

			processor.processFileAndScheduleJobs(jobXmlContent, jobName, getScheduler());
		} catch (Exception e) {
			getLog().error("Error scheduling jobs: " + e.getMessage(), e);
		}
	}

	public void removeJob(String jobName, String jobGroup) {
		try {
			JobKey jk = new JobKey(jobName, jobGroup);
			Scheduler scheduler = getScheduler();
			scheduler.pauseJob(jk);
			scheduler.deleteJob(jk);
		} catch (SchedulerException e) {
			getLog().error("Error remove scheduling jobs: " + e.getMessage(), e);
		}

	}

	public boolean isStarted() {
		return started;
	}

}

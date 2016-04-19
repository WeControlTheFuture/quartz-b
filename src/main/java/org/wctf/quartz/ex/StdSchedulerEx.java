package org.wctf.quartz.ex;

import org.quartz.core.QuartzScheduler;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.impl.StdScheduler;

public class StdSchedulerEx extends StdScheduler {
	private QuartzSchedulerResources rsrcs;

	public StdSchedulerEx(QuartzSchedulerResources rsrcs, QuartzScheduler sched) {
		super(sched);
		this.rsrcs = rsrcs;
	}

	public StdSchedulerEx(QuartzScheduler sched) {
		super(sched);
	}

	public QuartzSchedulerResources getRsrcs() {
		return rsrcs;
	}

}

package org.wctf.quartz.ex;

import org.quartz.Scheduler;
import org.quartz.core.QuartzScheduler;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.impl.StdSchedulerFactory;

public class StdSchedulerFactoryEx extends StdSchedulerFactory {

	@Override
	protected Scheduler instantiate(QuartzSchedulerResources rsrcs, QuartzScheduler qs) {
		return new StdSchedulerEx(rsrcs, qs);
	}

}

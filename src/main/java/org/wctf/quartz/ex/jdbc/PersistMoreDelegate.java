package org.wctf.quartz.ex.jdbc;

import java.sql.Connection;

import org.quartz.impl.jdbcjobstore.DriverDelegate;
import org.quartz.impl.jdbcjobstore.StdJDBCConstants;

public interface PersistMoreDelegate extends DriverDelegate, StdJDBCConstants {
	public static final String TABLE_TRIGGERS_HISTORY = "TRIGGERS_HISTORY";
	public static final String TABLE_JOB_DETAILS_HISTORY = "JOB_DETAILS_HISTORY";
	public static final String TABLE_JOB_EXECUTION_EXCEPTION = "JOB_EXECUTION_EXCEPTION";
	public static final String INSERT_JOB_EXECUTION_EXCEPTION = "INSERT INTO " + TABLE_PREFIX_SUBST + TABLE_JOB_EXECUTION_EXCEPTION + " VALUES(?,?,?)";
	public static final String INSERT_JOB_DETAIL_HISTORY = "INSERT INTO " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS_HISTORY + " SELECT * FROM " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " WHERE "
			+ COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST + " AND " + COL_JOB_NAME + " = ? AND " + COL_JOB_GROUP + " = ?";

	public static final String INSERT_TRIGGER_HISTORY = "INSERT INTO " + TABLE_PREFIX_SUBST + TABLE_TRIGGERS_HISTORY + " SELECT * FROM " + TABLE_PREFIX_SUBST + TABLE_TRIGGERS + " WHERE "
			+ COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST + " AND " + COL_TRIGGER_NAME + " = ? AND " + COL_TRIGGER_GROUP + " = ?";

	public void insertJobExecutionException(Connection conn, String msg, String exDetail);
}

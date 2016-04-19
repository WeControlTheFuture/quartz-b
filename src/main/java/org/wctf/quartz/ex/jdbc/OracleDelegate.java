package org.wctf.quartz.ex.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.impl.jdbcjobstore.StdJDBCDelegate;
import org.quartz.impl.jdbcjobstore.TriggerPersistenceDelegate;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleDelegate extends StdJDBCDelegate implements PersistMoreDelegate {
	private static final Logger log = LoggerFactory.getLogger(OracleDelegate.class);
	public static final String INSERT_ORACLE_JOB_DETAIL = "INSERT INTO " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " (" + COL_SCHEDULER_NAME + ", " + COL_JOB_NAME + ", " + COL_JOB_GROUP + ", "
			+ COL_DESCRIPTION + ", " + COL_JOB_CLASS + ", " + COL_IS_DURABLE + ", " + COL_IS_NONCONCURRENT + ", " + COL_IS_UPDATE_DATA + ", " + COL_REQUESTS_RECOVERY + ", " + COL_JOB_DATAMAP + ") "
			+ " VALUES(" + SCHED_NAME_SUBST + ", ?, ?, ?, ?, ?, ?, ?, ?, EMPTY_BLOB())";

	public static final String UPDATE_ORACLE_JOB_DETAIL = "UPDATE " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " SET " + COL_DESCRIPTION + " = ?, " + COL_JOB_CLASS + " = ?, " + COL_IS_DURABLE
			+ " = ?, " + COL_IS_NONCONCURRENT + " = ?, " + COL_IS_UPDATE_DATA + " = ?, " + COL_REQUESTS_RECOVERY + " = ?, " + COL_JOB_DATAMAP + " = EMPTY_BLOB() " + " WHERE " + COL_SCHEDULER_NAME
			+ " = " + SCHED_NAME_SUBST + " AND " + COL_JOB_NAME + " = ? AND " + COL_JOB_GROUP + " = ?";

	public static final String UPDATE_ORACLE_JOB_DETAIL_BLOB = "UPDATE " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " SET " + COL_JOB_DATAMAP + " = ? " + " WHERE " + COL_SCHEDULER_NAME + " = "
			+ SCHED_NAME_SUBST + " AND " + COL_JOB_NAME + " = ? AND " + COL_JOB_GROUP + " = ?";

	public static final String SELECT_ORACLE_JOB_DETAIL_BLOB = "SELECT " + COL_JOB_DATAMAP + " FROM " + TABLE_PREFIX_SUBST + TABLE_JOB_DETAILS + " WHERE " + COL_SCHEDULER_NAME + " = "
			+ SCHED_NAME_SUBST + " AND " + COL_JOB_NAME + " = ? AND " + COL_JOB_GROUP + " = ? FOR UPDATE";

	public static final String UPDATE_ORACLE_TRIGGER = "UPDATE " + TABLE_PREFIX_SUBST + TABLE_TRIGGERS + " SET " + COL_JOB_NAME + " = ?, " + COL_JOB_GROUP + " = ?, " + COL_DESCRIPTION + " = ?, "
			+ COL_NEXT_FIRE_TIME + " = ?, " + COL_PREV_FIRE_TIME + " = ?, " + COL_TRIGGER_STATE + " = ?, " + COL_TRIGGER_TYPE + " = ?, " + COL_START_TIME + " = ?, " + COL_END_TIME + " = ?, "
			+ COL_CALENDAR_NAME + " = ?, " + COL_MISFIRE_INSTRUCTION + " = ?, " + COL_PRIORITY + " = ? WHERE " + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST + " AND " + COL_TRIGGER_NAME
			+ " = ? AND " + COL_TRIGGER_GROUP + " = ?";

	public static final String SELECT_ORACLE_TRIGGER_JOB_DETAIL_BLOB = "SELECT " + COL_JOB_DATAMAP + " FROM " + TABLE_PREFIX_SUBST + TABLE_TRIGGERS + " WHERE " + COL_SCHEDULER_NAME + " = "
			+ SCHED_NAME_SUBST + " AND " + COL_TRIGGER_NAME + " = ? AND " + COL_TRIGGER_GROUP + " = ? FOR UPDATE";

	public static final String UPDATE_ORACLE_TRIGGER_JOB_DETAIL_BLOB = "UPDATE " + TABLE_PREFIX_SUBST + TABLE_TRIGGERS + " SET " + COL_JOB_DATAMAP + " = ? " + " WHERE " + COL_SCHEDULER_NAME + " = "
			+ SCHED_NAME_SUBST + " AND " + COL_TRIGGER_NAME + " = ? AND " + COL_TRIGGER_GROUP + " = ?";

	public static final String UPDATE_ORACLE_TRIGGER_JOB_DETAIL_EMPTY_BLOB = "UPDATE " + TABLE_PREFIX_SUBST + TABLE_TRIGGERS + " SET " + COL_JOB_DATAMAP + " = EMPTY_BLOB() " + " WHERE "
			+ COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST + " AND " + COL_TRIGGER_NAME + " = ? AND " + COL_TRIGGER_GROUP + " = ?";

	public static final String INSERT_ORACLE_CALENDAR = "INSERT INTO " + TABLE_PREFIX_SUBST + TABLE_CALENDARS + " (" + COL_SCHEDULER_NAME + ", " + COL_CALENDAR_NAME + ", " + COL_CALENDAR + ") "
			+ " VALUES(" + SCHED_NAME_SUBST + ", ?, EMPTY_BLOB())";

	public static final String SELECT_ORACLE_CALENDAR_BLOB = "SELECT " + COL_CALENDAR + " FROM " + TABLE_PREFIX_SUBST + TABLE_CALENDARS + " WHERE " + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST
			+ " AND " + COL_CALENDAR_NAME + " = ? FOR UPDATE";

	public static final String UPDATE_ORACLE_CALENDAR_BLOB = "UPDATE " + TABLE_PREFIX_SUBST + TABLE_CALENDARS + " SET " + COL_CALENDAR + " = ? " + " WHERE " + COL_SCHEDULER_NAME + " = "
			+ SCHED_NAME_SUBST + " AND " + COL_CALENDAR_NAME + " = ?";

	public OracleDelegate() {
		log.info("OracleDelegate call...............");
	}

	@Override
	protected Object getObjectFromBlob(ResultSet rs, String colName) throws ClassNotFoundException, IOException, SQLException {
		Object obj = null;
		InputStream binaryInput = rs.getBinaryStream(colName);
		if (binaryInput != null) {
			ObjectInputStream in = new ObjectInputStream(binaryInput);
			try {
				obj = in.readObject();
			} finally {
				in.close();
			}
		}
		return obj;
	}

	@Override
	public int insertJobDetail(Connection conn, JobDetail job) throws IOException, SQLException {
		ByteArrayOutputStream baos = serializeJobData(job.getJobDataMap());
		byte[] data = baos.toByteArray();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(rtp(INSERT_ORACLE_JOB_DETAIL));
			ps.setString(1, job.getKey().getName());
			ps.setString(2, job.getKey().getGroup());
			ps.setString(3, job.getDescription());
			ps.setString(4, job.getJobClass().getName());
			setBoolean(ps, 5, job.isDurable());
			setBoolean(ps, 6, job.isConcurrentExectionDisallowed());
			setBoolean(ps, 7, job.isPersistJobDataAfterExecution());
			setBoolean(ps, 8, job.requestsRecovery());

			ps.executeUpdate();
			ps.close();

			ps = conn.prepareStatement(rtp(SELECT_ORACLE_JOB_DETAIL_BLOB));
			ps.setString(1, job.getKey().getName());
			ps.setString(2, job.getKey().getGroup());

			rs = ps.executeQuery();

			int res = 0;

			Blob dbBlob = null;
			if (rs.next()) {
				dbBlob = writeDataToBlob(rs, 1, data);
			} else {
				return res;
			}

			rs.close();
			ps.close();

			ps = conn.prepareStatement(rtp(UPDATE_ORACLE_JOB_DETAIL_BLOB));
			ps.setBlob(1, dbBlob);
			ps.setString(2, job.getKey().getName());
			ps.setString(3, job.getKey().getGroup());
			res = ps.executeUpdate();
			return res;
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
		}

	}

	@Override
	protected Object getJobDataFromBlob(ResultSet rs, String colName) throws ClassNotFoundException, IOException, SQLException {

		if (canUseProperties()) {
			InputStream binaryInput = rs.getBinaryStream(colName);
			return binaryInput;
		}

		return getObjectFromBlob(rs, colName);
	}

	@Override
	public int updateJobDetail(Connection conn, JobDetail job) throws IOException, SQLException {

		ByteArrayOutputStream baos = serializeJobData(job.getJobDataMap());
		byte[] data = baos.toByteArray();

		PreparedStatement ps = null;
		PreparedStatement ps2 = null;
		ResultSet rs = null;

		try {
			ps = conn.prepareStatement(rtp(UPDATE_ORACLE_JOB_DETAIL));
			ps.setString(1, job.getDescription());
			ps.setString(2, job.getJobClass().getName());
			setBoolean(ps, 3, job.isDurable());
			setBoolean(ps, 4, job.isConcurrentExectionDisallowed());
			setBoolean(ps, 5, job.isPersistJobDataAfterExecution());
			setBoolean(ps, 6, job.requestsRecovery());
			ps.setString(7, job.getKey().getName());
			ps.setString(8, job.getKey().getGroup());

			ps.executeUpdate();
			ps.close();

			ps = conn.prepareStatement(rtp(SELECT_ORACLE_JOB_DETAIL_BLOB));
			ps.setString(1, job.getKey().getName());
			ps.setString(2, job.getKey().getGroup());

			rs = ps.executeQuery();

			int res = 0;

			if (rs.next()) {
				Blob dbBlob = writeDataToBlob(rs, 1, data);
				ps2 = conn.prepareStatement(rtp(UPDATE_ORACLE_JOB_DETAIL_BLOB));

				ps2.setBlob(1, dbBlob);
				ps2.setString(2, job.getKey().getName());
				ps2.setString(3, job.getKey().getGroup());

				res = ps2.executeUpdate();
			}

			return res;

		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeStatement(ps2);
		}
	}

	@Override
	public int insertTrigger(Connection conn, OperableTrigger trigger, String state, JobDetail jobDetail) throws SQLException, IOException {
		System.out.println("insert trigger.........................");
		byte[] data = null;
		if (trigger.getJobDataMap().size() > 0) {
			data = serializeJobData(trigger.getJobDataMap()).toByteArray();
		}

		PreparedStatement ps = null;
		ResultSet rs = null;

		int insertResult = 0;

		try {
			ps = conn.prepareStatement(rtp(INSERT_TRIGGER));
			ps.setString(1, trigger.getKey().getName());
			ps.setString(2, trigger.getKey().getGroup());
			ps.setString(3, trigger.getJobKey().getName());
			ps.setString(4, trigger.getJobKey().getGroup());
			ps.setString(5, trigger.getDescription());
			ps.setBigDecimal(6, new BigDecimal(String.valueOf(trigger.getNextFireTime().getTime())));
			long prevFireTime = -1;
			if (trigger.getPreviousFireTime() != null) {
				prevFireTime = trigger.getPreviousFireTime().getTime();
			}
			ps.setBigDecimal(7, new BigDecimal(String.valueOf(prevFireTime)));
			ps.setString(8, state);

			TriggerPersistenceDelegate tDel = findTriggerPersistenceDelegate(trigger);

			String type = TTYPE_BLOB;
			if (tDel != null)
				type = tDel.getHandledTriggerTypeDiscriminator();
			ps.setString(9, type);

			ps.setBigDecimal(10, new BigDecimal(String.valueOf(trigger.getStartTime().getTime())));
			long endTime = 0;
			if (trigger.getEndTime() != null) {
				endTime = trigger.getEndTime().getTime();
			}
			ps.setBigDecimal(11, new BigDecimal(String.valueOf(endTime)));
			ps.setString(12, trigger.getCalendarName());
			ps.setInt(13, trigger.getMisfireInstruction());
			ps.setBinaryStream(14, null, 0);
			ps.setInt(15, trigger.getPriority());

			insertResult = ps.executeUpdate();

			if (data != null) {
				ps.close();

				ps = conn.prepareStatement(rtp(UPDATE_ORACLE_TRIGGER_JOB_DETAIL_EMPTY_BLOB));
				ps.setString(1, trigger.getKey().getName());
				ps.setString(2, trigger.getKey().getGroup());
				ps.executeUpdate();
				ps.close();

				ps = conn.prepareStatement(rtp(SELECT_ORACLE_TRIGGER_JOB_DETAIL_BLOB));
				ps.setString(1, trigger.getKey().getName());
				ps.setString(2, trigger.getKey().getGroup());

				rs = ps.executeQuery();

				Blob dbBlob = null;
				if (rs.next()) {
					dbBlob = writeDataToBlob(rs, 1, data);
				} else {
					return 0;
				}

				rs.close();
				ps.close();

				ps = conn.prepareStatement(rtp(UPDATE_ORACLE_TRIGGER_JOB_DETAIL_BLOB));
				ps.setBlob(1, dbBlob);
				ps.setString(2, trigger.getKey().getName());
				ps.setString(3, trigger.getKey().getGroup());

				ps.executeUpdate();
			}

			if (tDel == null)
				insertBlobTrigger(conn, trigger);
			else
				tDel.insertExtendedTriggerProperties(conn, trigger, state, jobDetail);

		} finally {
			closeResultSet(rs);
			closeStatement(ps);
		}

		return insertResult;
	}

	@Override
	public int updateTrigger(Connection conn, OperableTrigger trigger, String state, JobDetail jobDetail) throws SQLException, IOException {

		// save some clock cycles by unnecessarily writing job data blob ...
		boolean updateJobData = trigger.getJobDataMap().isDirty();
		byte[] data = null;
		if (updateJobData && trigger.getJobDataMap().size() > 0) {
			data = serializeJobData(trigger.getJobDataMap()).toByteArray();
		}

		PreparedStatement ps = null;
		PreparedStatement ps2 = null;
		ResultSet rs = null;

		int insertResult = 0;

		try {
			ps = conn.prepareStatement(rtp(UPDATE_ORACLE_TRIGGER));

			ps.setString(1, trigger.getJobKey().getName());
			ps.setString(2, trigger.getJobKey().getGroup());
			ps.setString(3, trigger.getDescription());
			long nextFireTime = -1;
			if (trigger.getNextFireTime() != null) {
				nextFireTime = trigger.getNextFireTime().getTime();
			}
			ps.setBigDecimal(4, new BigDecimal(String.valueOf(nextFireTime)));
			long prevFireTime = -1;
			if (trigger.getPreviousFireTime() != null) {
				prevFireTime = trigger.getPreviousFireTime().getTime();
			}
			ps.setBigDecimal(5, new BigDecimal(String.valueOf(prevFireTime)));
			ps.setString(6, state);

			TriggerPersistenceDelegate tDel = findTriggerPersistenceDelegate(trigger);

			String type = TTYPE_BLOB;
			if (tDel != null)
				type = tDel.getHandledTriggerTypeDiscriminator();

			ps.setString(7, type);

			ps.setBigDecimal(8, new BigDecimal(String.valueOf(trigger.getStartTime().getTime())));
			long endTime = 0;
			if (trigger.getEndTime() != null) {
				endTime = trigger.getEndTime().getTime();
			}
			ps.setBigDecimal(9, new BigDecimal(String.valueOf(endTime)));
			ps.setString(10, trigger.getCalendarName());
			ps.setInt(11, trigger.getMisfireInstruction());
			ps.setInt(12, trigger.getPriority());
			ps.setString(13, trigger.getKey().getName());
			ps.setString(14, trigger.getKey().getGroup());

			insertResult = ps.executeUpdate();

			if (updateJobData) {
				ps.close();

				ps = conn.prepareStatement(rtp(UPDATE_ORACLE_TRIGGER_JOB_DETAIL_EMPTY_BLOB));
				ps.setString(1, trigger.getKey().getName());
				ps.setString(2, trigger.getKey().getGroup());
				ps.executeUpdate();
				ps.close();

				ps = conn.prepareStatement(rtp(SELECT_ORACLE_TRIGGER_JOB_DETAIL_BLOB));
				ps.setString(1, trigger.getKey().getName());
				ps.setString(2, trigger.getKey().getGroup());

				rs = ps.executeQuery();

				if (rs.next()) {
					Blob dbBlob = writeDataToBlob(rs, 1, data);
					ps2 = conn.prepareStatement(rtp(UPDATE_ORACLE_TRIGGER_JOB_DETAIL_BLOB));

					ps2.setBlob(1, dbBlob);
					ps2.setString(2, trigger.getKey().getName());
					ps2.setString(3, trigger.getKey().getGroup());

					ps2.executeUpdate();
				}
			}

			if (tDel == null)
				updateBlobTrigger(conn, trigger);
			else
				tDel.updateExtendedTriggerProperties(conn, trigger, state, jobDetail);

		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeStatement(ps2);
		}

		return insertResult;
	}

	@Override
	public int insertCalendar(Connection conn, String calendarName, Calendar calendar) throws IOException, SQLException {
		ByteArrayOutputStream baos = serializeObject(calendar);

		PreparedStatement ps = null;
		PreparedStatement ps2 = null;
		ResultSet rs = null;

		try {
			ps = conn.prepareStatement(rtp(INSERT_ORACLE_CALENDAR));
			ps.setString(1, calendarName);

			ps.executeUpdate();
			ps.close();

			ps = conn.prepareStatement(rtp(SELECT_ORACLE_CALENDAR_BLOB));
			ps.setString(1, calendarName);

			rs = ps.executeQuery();

			if (rs.next()) {
				Blob dbBlob = writeDataToBlob(rs, 1, baos.toByteArray());
				ps2 = conn.prepareStatement(rtp(UPDATE_ORACLE_CALENDAR_BLOB));

				ps2.setBlob(1, dbBlob);
				ps2.setString(2, calendarName);

				return ps2.executeUpdate();
			}

			return 0;

		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeStatement(ps2);
		}
	}

	@Override
	public int updateCalendar(Connection conn, String calendarName, Calendar calendar) throws IOException, SQLException {
		ByteArrayOutputStream baos = serializeObject(calendar);

		PreparedStatement ps = null;
		PreparedStatement ps2 = null;
		ResultSet rs = null;

		try {
			ps = conn.prepareStatement(rtp(SELECT_ORACLE_CALENDAR_BLOB));
			ps.setString(1, calendarName);

			rs = ps.executeQuery();

			if (rs.next()) {
				Blob dbBlob = writeDataToBlob(rs, 1, baos.toByteArray());
				ps2 = conn.prepareStatement(rtp(UPDATE_ORACLE_CALENDAR_BLOB));

				ps2.setBlob(1, dbBlob);
				ps2.setString(2, calendarName);

				return ps2.executeUpdate();
			}

			return 0;

		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeStatement(ps2);
		}
	}

	@Override
	public int updateJobData(Connection conn, JobDetail job) throws IOException, SQLException {

		ByteArrayOutputStream baos = serializeJobData(job.getJobDataMap());
		byte[] data = baos.toByteArray();

		PreparedStatement ps = null;
		PreparedStatement ps2 = null;
		ResultSet rs = null;

		try {
			ps = conn.prepareStatement(rtp(SELECT_ORACLE_JOB_DETAIL_BLOB));
			ps.setString(1, job.getKey().getName());
			ps.setString(2, job.getKey().getGroup());

			rs = ps.executeQuery();

			int res = 0;

			if (rs.next()) {
				Blob dbBlob = writeDataToBlob(rs, 1, data);
				ps2 = conn.prepareStatement(rtp(UPDATE_ORACLE_JOB_DETAIL_BLOB));

				ps2.setBlob(1, dbBlob);
				ps2.setString(2, job.getKey().getName());
				ps2.setString(3, job.getKey().getGroup());

				res = ps2.executeUpdate();
			}

			return res;
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeStatement(ps2);
		}
	}

	protected Blob writeDataToBlob(ResultSet rs, int column, byte[] data) throws SQLException {

		Blob blob = rs.getBlob(column); // get blob

		if (blob == null) {
			throw new SQLException("Driver's Blob representation is null!");
		}

		return null;
		// 注释掉，由于orcle的包实在是难以从maven上刷下来，谁要用，谁打开注释，加上oracle的jdbc包
		// if (blob instanceof oracle.sql.BLOB) { // is it an oracle blob?
		// ((oracle.sql.BLOB) blob).putBytes(1, data);
		// ((oracle.sql.BLOB) blob).trim(data.length);
		// return blob;
		// } else {
		// throw new SQLException("Driver's Blob representation is of an
		// unsupported type: " + blob.getClass().getName());
		// }
	}

	@Override
	public int deleteFiredTriggers(Connection conn) throws SQLException {
		return super.deleteFiredTriggers(conn);
	}

	@Override
	public int deleteFiredTriggers(Connection conn, String theInstanceId) throws SQLException {
		return super.deleteFiredTriggers(conn, theInstanceId);
	}

	@Override
	public int deleteJobDetail(Connection conn, JobKey jobKey) throws SQLException {
		// save jobdetail to history before deleted
		// PreparedStatement ps = null;
		// int insertNum = 0;
		// try {
		// ps = conn.prepareStatement(rtp(INSERT_JOB_DETAIL_HISTORY));
		// ps.setString(1, jobKey.getName());
		// ps.setString(2, jobKey.getGroup());
		// insertNum = ps.executeUpdate();
		// } finally {
		// closeStatement(ps);
		// }
		int delNum = super.deleteJobDetail(conn, jobKey);
		// if (insertNum != delNum)
		// log.error("insert history num not equal delete num,insert num is" +
		// insertNum + " delete num is" + delNum);
		return delNum;
	}

	@Override
	public int deleteBlobTrigger(Connection conn, TriggerKey triggerKey) throws SQLException {
		return super.deleteBlobTrigger(conn, triggerKey);
	}

	/*
	 * 扩展，在quartz删除触发器之前，先将触发器移到历史表
	 * 
	 * @see org.quartz.impl.jdbcjobstore.StdJDBCDelegate#deleteTrigger(java.sql.
	 * Connection, org.quartz.TriggerKey)
	 */
	public int deleteTrigger(Connection conn, TriggerKey triggerKey) throws SQLException {
		// save trigger to history before deleted
		PreparedStatement ps = null;
		int insertNum = 0;
		try {
			ps = conn.prepareStatement(rtp(INSERT_TRIGGER_HISTORY));
			ps.setString(1, triggerKey.getName());
			ps.setString(2, triggerKey.getGroup());
			insertNum = ps.executeUpdate();
		} finally {
			closeStatement(ps);
		}
		int delNum = super.deleteTrigger(conn, triggerKey);
		if (insertNum != delNum)
			log.error("insert history num not equal delete num,insert num is" + insertNum + " delete num is" + delNum);
		return delNum;
	}

	@Override
	protected void deleteTriggerExtension(Connection conn, TriggerKey triggerKey) throws SQLException {
		super.deleteTriggerExtension(conn, triggerKey);
	}

	@Override
	public int deletePausedTriggerGroup(Connection conn, String groupName) throws SQLException {
		return super.deletePausedTriggerGroup(conn, groupName);
	}

	@Override
	public int deletePausedTriggerGroup(Connection conn, GroupMatcher<TriggerKey> matcher) throws SQLException {
		return super.deletePausedTriggerGroup(conn, matcher);
	}

	@Override
	public int deleteAllPausedTriggerGroups(Connection conn) throws SQLException {
		return super.deleteAllPausedTriggerGroups(conn);
	}

	@Override
	public int deleteCalendar(Connection conn, String calendarName) throws SQLException {
		return super.deleteCalendar(conn, calendarName);
	}

	@Override
	public int deleteFiredTrigger(Connection conn, String entryId) throws SQLException {
		return super.deleteFiredTrigger(conn, entryId);
	}

	@Override
	public int deleteSchedulerState(Connection conn, String theInstanceId) throws SQLException {
		return super.deleteSchedulerState(conn, theInstanceId);
	}

	@Override
	public void insertJobExecutionException(Connection conn, String msg, String exDetail) {
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(rtp(INSERT_JOB_EXECUTION_EXCEPTION));
			ps.setString(1, msg);
			ps.setBigDecimal(2, new BigDecimal(System.currentTimeMillis()));
			ps.setString(3, exDetail);
			ps.execute();
			ps.close();
		} catch (SQLException e) {
			closeStatement(ps);
		}

	}
}

package org.wctf.quartz.ex.jdbc;

import java.sql.Connection;

import org.quartz.impl.jdbcjobstore.StdJDBCDelegate;

public class MySqlDelegate extends StdJDBCDelegate implements PersistMoreDelegate{

	@Override
	public void insertJobExecutionException(Connection conn, String msg, String exDetail) {
		
	}

}

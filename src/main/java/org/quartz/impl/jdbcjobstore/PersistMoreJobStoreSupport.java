package org.quartz.impl.jdbcjobstore;

import java.sql.Connection;

import org.quartz.JobPersistenceException;
import org.wctf.quartz.ex.jdbc.OracleDelegate;
import org.wctf.quartz.ex.jdbc.PersistMoreDelegate;


public class PersistMoreJobStoreSupport extends JobStoreTX {

	protected PersistMoreDelegate persistMoreDelegate;

	protected Class<? extends PersistMoreDelegate> delegateClass = OracleDelegate.class;

	@Override
	protected PersistMoreDelegate getDelegate() throws NoSuchDelegateException {
		synchronized (this) {
			if (null == persistMoreDelegate) {
				try {
					if (delegateClassName != null) {
						delegateClass = getClassLoadHelper().loadClass(delegateClassName, PersistMoreDelegate.class);
					}
					persistMoreDelegate = delegateClass.newInstance();
					persistMoreDelegate.initialize(getLog(), tablePrefix, instanceName, instanceId, getClassLoadHelper(), canUseProperties(), getDriverDelegateInitString());
				} catch (InstantiationException e) {
					throw new NoSuchDelegateException("Couldn't create delegate: " + e.getMessage(), e);
				} catch (IllegalAccessException e) {
					throw new NoSuchDelegateException("Couldn't create delegate: " + e.getMessage(), e);
				} catch (ClassNotFoundException e) {
					throw new NoSuchDelegateException("Couldn't load delegate class: " + e.getMessage(), e);
				}
			}
			return persistMoreDelegate;
		}
	}

	public void storeJobExecutionException(final String msg, final String exDetail) throws JobPersistenceException {
		executeInNonManagedTXLock(null, new VoidTransactionCallback() {
			@Override
			public void executeVoid(Connection conn) throws JobPersistenceException {
				getDelegate().insertJobExecutionException(conn, msg, exDetail);
			}
		}, null);
	}
}

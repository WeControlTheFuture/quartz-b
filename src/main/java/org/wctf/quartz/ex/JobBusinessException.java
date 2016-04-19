package org.wctf.quartz.ex;

/**
 * 涉及到定时任务中业务异常，统一抛出此异常
 * 
 * @author 毕希研
 * 
 */
public class JobBusinessException extends RuntimeException {

	public JobBusinessException() {
		super();
	}

	public JobBusinessException(String message) {
		super(message);
	}

	public JobBusinessException(String message, Throwable cause) {
		super(message, cause);
	}

	public JobBusinessException(Throwable cause) {
		super(cause);
	}
}

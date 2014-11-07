package dfs.exceptions;

import java.rmi.RemoteException;

public class EndsWithException extends RemoteException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -397610393834951224L;

	public EndsWithException() {
		super();
	}
	
	public EndsWithException(String errorMsg) {
		super(errorMsg);
	}
	
	
}

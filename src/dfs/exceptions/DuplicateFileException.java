package dfs.exceptions;

import java.rmi.RemoteException;

public class DuplicateFileException extends RemoteException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -397610393834951224L;

	public DuplicateFileException() {
		super();
	}
	
	public DuplicateFileException(String errorMsg) {
		super(errorMsg);
	}
	
	
}

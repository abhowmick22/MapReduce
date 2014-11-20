/**
 * Remove exception when user adds an already existing file to DFS.
 */
package dfs.exceptions;

import java.rmi.RemoteException;

public class DuplicateFileException extends RemoteException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6353408710649083701L;

	public DuplicateFileException() {
		super("File already exists. Please delete it first.");
	}
	
	public DuplicateFileException(String errorMsg) {
		super(errorMsg);
	}	
}

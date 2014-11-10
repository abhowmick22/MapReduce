/** 
 * Remote exception when a user requested file is not found on DFS.
 */
package dfs.exceptions;

import java.rmi.RemoteException;

public class InvalidPathException extends RemoteException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4411928289510660535L;

	public InvalidPathException() {
		super("Invalid DFS path: \n" +
				"1. Check if file name ends with \".txt\" \n" +
				"2. No directory name ends with \".txt\" \n" +
				"3. Username is correct in the path");
	}
	
	public InvalidPathException(String errorMsg) {
		super(errorMsg);
	}

}

/** 
 * Remote exception when a user requested file is not found on DFS.
 */

package dfs.exceptions;

import java.rmi.RemoteException;

public class DfsFileNotFoundException extends RemoteException{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6169521293005561001L;

	public DfsFileNotFoundException() {
		super("File not found on DFS.");
	}
	
	public DfsFileNotFoundException(String errorMsg) {
		super(errorMsg);
	}
}

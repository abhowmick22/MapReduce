/** 
 * Remote exception when a user requested file is not found on DFS.
 */

package dfs.exceptions;

import java.rmi.RemoteException;

public class DfsFileNotFound extends RemoteException{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6169521293005561001L;

	public DfsFileNotFound() {
		super();
	}
	
	public DfsFileNotFound(String errorMsg) {
		super(errorMsg);
	}
}

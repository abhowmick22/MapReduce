package datanode.exceptions;

import java.rmi.RemoteException;

public class BlockNotFoundException extends RemoteException {
    
    /**
     * 
     */
    private static final long serialVersionUID = -5881856004268847284L;

    public BlockNotFoundException() {
        super("File block not found.");
    }
}


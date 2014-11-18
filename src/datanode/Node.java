package datanode;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Node extends Remote
{
    public boolean createFile(String path) throws RemoteException;
    public boolean writeToFile(String path, byte[] bytes, int start) throws RemoteException;
    public byte[] getFile(String path, int start) throws RemoteException;
}

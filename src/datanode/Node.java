package datanode;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Node extends Remote
{
    public boolean createFile(String path) throws RemoteException;
    public boolean writeToFile(String path, byte[] bytes, int start) throws RemoteException;
    public byte[] getFile(String path, int start) throws RemoteException;
    public boolean transferBlockTo(Node destNode, String path) throws RemoteException;
    public String getNodeName() throws RemoteException;
    public void deleteFile(String path) throws RemoteException;    
}

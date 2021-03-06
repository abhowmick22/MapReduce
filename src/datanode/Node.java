package datanode;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Node extends Remote
{
    public boolean createFile(String path) throws RemoteException;
    public boolean writeToFile(String path, byte[] bytes, int start, int count) throws RemoteException;
    
    
    public byte[] getFile(String path, int start) throws RemoteException;
    public boolean transferBlockTo(Node destNode, String path) throws RemoteException;
    public String getNodeName() throws RemoteException;
    public void deleteFile(String path) throws RemoteException;
    public void sendJarFile(String jarPath, byte[] bytes, int start, int count) throws RemoteException;
    public void testRunJar(String jarPath, String mapClassName) throws RemoteException;
    
}

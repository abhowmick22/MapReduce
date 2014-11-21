package clientapi;

import java.rmi.RemoteException;

import dfs.InputSplit;

public interface ClientApi {
    public boolean checkFileExists(String dfsPath) throws RemoteException;
    public void addFileToDfs(String path, String dfsPath, InputSplit inputSplit, boolean overwrite);
    public void getFileFromDfs(String dfsPath, String outputPath);
    public String printDFSStructure();
    public void deleteFileFromDfs(String dfsPath);
}

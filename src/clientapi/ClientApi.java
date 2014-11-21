package clientapi;

import dfs.InputSplit;

public interface ClientApi {
        
    public boolean checkFileExists(String dfsPath);
    public void addFileToDfs(String path, String dfsPath, InputSplit inputSplit, boolean overwrite);
    public void getFileFromDfs(String dfsPath, String outputPath);
    public void getDirFromDfs(String dfsPath, String outputPath);
    public String printDFSStructure();
    public void deleteFileFromDfs(String dfsPath);
    public void startMapReduce(String jarPath);
}

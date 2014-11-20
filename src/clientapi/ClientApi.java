package clientapi;

import dfs.InputSplit;

public interface ClientApi {
    public void addFileToDfs(String path, String dfsPath, InputSplit inputSplit);
    public void getFileFromDfs(String dfsPath, String outputPath);
    public String printDFSStructure();
}

package clientapi;

import dfs.InputSplit;

public interface ClientApi {
    public void addFileToDFS(String path, String dfsPath, InputSplit inputSplit);
    public String printDFSStructure();
}

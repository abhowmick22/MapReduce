package clientapi;

import dfs.InputSplit;

public interface ClientApi {
    public void addFileToDFS(String path, InputSplit inputSplit);
}

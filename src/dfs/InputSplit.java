package dfs;

import java.io.Serializable;

public class InputSplit implements Serializable
{
    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = -2920179206087082516L;
    private String delimiter;
    private int bytes;
    private String splitParam;
    
    public InputSplit(String delimiter) {
        this.splitParam = "c";      //"c" for character sequence
        this.delimiter = delimiter;
        this.bytes = -1;
    }
    
    public InputSplit(int bytes) {
        this.splitParam = "b";      //"b" for bytes
        this.delimiter = "";
        this.bytes = bytes;
    }
    
    public String getDelimiter() {
        return this.delimiter;
    }
    
    public int getBytes() {
        return this.bytes;
    }
    
    public String getSplitParam() {
        return this.splitParam;
    }
    
    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }
    
    public void setBytes(int bytes) {
        this.bytes = bytes;
    }
}

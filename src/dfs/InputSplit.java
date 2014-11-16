package dfs;

public class InputSplit
{
    private char delimiter;
    private int bytes;
    private String splitParam;
    
    public InputSplit(char delimiter) {
        this.splitParam = "c";
        this.delimiter = delimiter;
        this.bytes = -1;
    }
    
    public InputSplit(int bytes) {
        this.splitParam = "b";
        this.delimiter = 0;
        this.bytes = bytes;
    }
    
    public char getDelimiter() {
        return this.delimiter;
    }
    
    public int getBytes() {
        return this.bytes;
    }
    
    public String getSplitParam() {
        return this.splitParam;
    }
    
    public void setDelimiter(char delimiter) {
        this.delimiter = delimiter;
    }
    
    public void setBytes(int bytes) {
        this.bytes = bytes;
    }
}

/**
 * The main class for the distributed file system.
 */
package dfs;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class DfsInit
{
    private Map<String, Integer> dataNodeLoad;     //maintains a map between datanode and load on it
                                                   //in terms of number of blocks stored on it
    private int repFactor;                         //replication factor
    
    
    public static void main(String[] args) {
        new DfsInit(new ArrayList<String>(), 2);
        //read config file to know how many and which datanodes are added
        
    }
    
    
    public DfsInit(List<String> dataNodeList, int repFactor) {
        //initialize data node map with initial load of 0
        this.dataNodeLoad = new HashMap<String, Integer>();
        for(String dataNode: dataNodeList) {
            this.dataNodeLoad.put(dataNode, 0);
        }
        this.repFactor = repFactor;
        //TODO: Use this for sending list of datanodes to distribute the file on
//        Map<String, Integer> map = new TreeMap<String, Integer>(new LoadComparator(dataNodeLoad));
//        map.putAll(dataNodeLoad);
//        
//        for(Entry<String, Integer>entry: map.entrySet()) {
//            System.out.println(entry.getKey()+"-"+entry.getValue());
//        }

    }
    
    /**
     * Returns the data node with minimum load (in terms of disk space)
     * @return Data node with minimum load.
     */
    private String getMinLoad() {
        int min = Integer.MAX_VALUE;
        String minNode = null;
        for(Entry<String, Integer> entry : this.dataNodeLoad.entrySet()) {
            if(entry.getValue() < min) {
                min = entry.getValue();
                minNode = entry.getKey();
            }
        }
        
        return minNode;
    }
    
    private class LoadComparator implements Comparator<String> {
        private Map<String, Integer> map;        
        public LoadComparator(Map<String, Integer> map) {
            this.map = map;
        }        
        public int compare(String key1, String key2)
        {
            return map.get(key1) >= map.get(key2) ? 1 : -1;            
        }        
    }
    
}

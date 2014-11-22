package mapred.tests;

import java.util.List;

import mapred.interfaces.Mapper;
import mapred.types.Pair;

public class GeoLocMapper implements Mapper {

    @Override
    public void map(String record, List<Pair<String, String>> output) {
        
        String[] keys = record.split("[\t ]");  //split by tab and space 
        Pair<String, String> p = null;
        if(keys.length<5) {
            return;
        }
        p = new Pair<String, String>();
        double lat = Double.parseDouble(keys[2]);
        double lon = Double.parseDouble(keys[3]);                        
        if(lat>0 && lon>0) {
            p.setFirst("NorthEast");
        } else if(lat>0 && lon<0) {
            p.setFirst("NorthWest");
        } else if(lat<0 && lon>0) {
            p.setFirst("SouthEast");
        } else {
            p.setFirst("SouthWest");
        }
        p.setSecond("1");
        output.add(p);
        
    }
    
    /* TODO: below is for test, to be removed*/
    public static void main(String args[]) throws Exception {
        java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader("examples/geo_en_final.txt"));
        List<Pair<String, String>> output = new java.util.ArrayList<Pair<String, String>>();
        GeoLocMapper mapper = new GeoLocMapper();
        for(int i=0;i<5;i++) {
            mapper.map(br.readLine(), output);
        }
        for(Pair<String, String> entry: output) {
            System.out.println(entry.getFirst()+", "+entry.getSecond());
        }
        br.close();
    }
    
}

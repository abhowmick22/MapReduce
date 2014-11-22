package mapred.tests;

import java.util.List;
import java.util.ListIterator;

import mapred.interfaces.Reducer;

public class GeoLocReducer implements Reducer {

    @Override
    public String reduce(List<String> input) {
        int sum = 0;
        String s = null;
        ListIterator<String> it = input.listIterator();
        
        while(it.hasNext()){
            s = it.next();
            sum += Integer.parseInt(s);
        }
        return String.valueOf(sum);
    }

}

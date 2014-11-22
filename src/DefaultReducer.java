

import java.io.Serializable;
import java.util.List;
import java.util.ListIterator;

import mapred.interfaces.Reducer;
import mapred.types.Pair;

/*
 * This default reducer is the reducer for inverted index 
 */

public class DefaultReducer implements Reducer, Serializable {

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

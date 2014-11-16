package mapred.tests;

import java.io.Serializable;
import java.util.List;

import mapred.interfaces.Reducer;
import mapred.types.Pair;

/*
 * This default reducer does inverted index 
 */

public class DefaultReducer implements Reducer, Serializable {

	@Override
	public void reduce(List<Pair<String>> input, List<Pair<String>> output) {
		// Do nothing
		return;
	}

}

package mapred.interfaces;

import java.util.List;

import mapred.types.Pair;

public interface Combiner {
	
	// the combiner method
	public void combine(List<Pair<String, String>> input, List<Pair<String, String>> output);

}

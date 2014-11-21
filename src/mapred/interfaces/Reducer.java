package mapred.interfaces;

import java.util.List;

/*
 * Interface for a reducer
 * It takes in a <String, String> pair for the KV input and emits a KV pair as the output
 * It is the job of the application programmer to provide appropriate conversions to/from
 * other types of KV pairs to <String, String> pair
 * Our recordwriter will only write strings to the files
 * 
 * Do we need the generic type parameters ?
 * Reducer<K1, V1, K2, V2>
 */


public interface Reducer {
	
	// the reduce method
	public String reduce(List<String> input);

}

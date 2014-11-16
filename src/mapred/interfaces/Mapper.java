package mapred.interfaces;

import mapred.OutputSet;

/*
 * Interface for a mapper
 * It takes in a <String, String> pair for the KV input and emits a KV pair as the output
 * It is the job of the application programmer to provide appropriate conversions to/from
 * other types of KV pairs to <String, String> pair
 * Our recordreader will only read in strings from the files and provide output intermediate
 * records only of type HashMap<String, List<String> >
 * 
 * Do we need the generic type parameters ?
 * Mapper<K1, V1, K2, V2>
 */

public interface Mapper {
	
	// the map method
	public void map(String record, OutputSet output);

}

package mapred;

import java.util.HashMap;
import java.util.List;

/*
 * This object (per mapper) collects all the key, value pairs produced by the mapper
 * It stores a list of values for every type of key in the output
 */

public class OutputSet {
	
	// the buffer that contains the intermediate key-value lists (output of map)
	private HashMap<String, List<String> > intermediateBuf;
	// the map that contains the lengths for each key
	private HashMap<String, Integer> keyLens;

}

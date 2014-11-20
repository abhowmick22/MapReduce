package mapred.tests;

import java.io.Serializable;
import java.util.List;


import mapred.interfaces.Mapper;
import mapred.types.Pair;

/*
 * This default mapper does inverted index
 * Emits {wprd, "1"} pairs
 */

public class DefaultMapper implements Mapper, Serializable {

	@Override
	public void map(String record, List<Pair<String, String>> output) {
		String[] keys = record.split("\\s*,\\s*");	// split by any number of consecutive spaces
		Pair<String, String> p = null;
		for (String k : keys){
			p = new Pair<String, String>();
			p.setFirst(k);
			p.setSecond("1");
			output.add(p);
		}
	}

}

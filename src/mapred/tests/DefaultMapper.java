package mapred.tests;

import java.io.Serializable;
import java.util.List;


import mapred.interfaces.Mapper;
import mapred.types.Pair;

/*
 * This default mapper does inverted index
 * Emits {word, "1"} pairs
 */

public class DefaultMapper implements Mapper, Serializable {

	@Override
	public void map(String record, List<Pair<String, String>> output) {
		System.out.println(record);
		System.out.println(output.get(0).getFirst());

		// TODO: Add more delimiters
		String[] keys = record.split("\\s*,\\s*");	// split by any number of consecutive spaces
		Pair<String, String> p = null;
		for (String k : keys){
			// if key k is ending the document then it may contain \n
			// strip all such keys of trailing \n
			int last = k.length()-1;
			if(k.charAt(last) == '\n')	{
				k = k.substring(0, last);
			}
			
			p = new Pair<String, String>();
			p.setFirst(k);
			p.setSecond("1");
			output.add(p);
		}
		
	}

}

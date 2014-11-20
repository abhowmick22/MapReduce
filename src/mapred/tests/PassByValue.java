package mapred.tests;

import java.util.ArrayList;
import java.util.List;

public class PassByValue {
	
	public static void main(String[] args){
		int a = 5;
		Integer b = 5;
		List<Integer> list = new ArrayList<Integer>();
		list.add(b);
		increment(list);
		System.out.println("main " + list.get(0));
	}
	
	public static void increment(List<Integer> list){
		//Integer b = list.get(0);
		list.add(0, 8);
		System.out.println("method " + list.get(0));
		
	}

}

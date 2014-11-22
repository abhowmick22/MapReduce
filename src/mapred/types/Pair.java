package mapred.types;

import java.io.Serializable;

/*
 * This is a pair type, which is required in many places 
 */

public class Pair<T, Y> implements Serializable {
	
	private static final long serialVersionUID = 1L;
	// first element
	private T first;
	// second element
	private Y second;
	
	public T getFirst(){
		return this.first;
	}
	
	public Y getSecond(){
		return this.second;
	}
	
	public void setFirst(T first){
		this.first = first;
	}
	
	public void setSecond(Y second){
		this.second = second;
	}

}

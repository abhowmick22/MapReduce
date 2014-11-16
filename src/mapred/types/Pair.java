package mapred.types;

/*
 * This is a pair type, which is required in many places 
 */

public class Pair<T> {
	// first element
	private T first;
	// second element
	private T second;
	
	public T getFirst(){
		return this.first;
	}
	
	public T getSecond(){
		return this.second;
	}
	
	public void setFirst(T first){
		this.first = first;
	}
	
	public void setSecond(T second){
		this.second = second;
	}
}

package mapred.interfaces;

public interface Combiner {
	
	// the combiner method
	public void combiner(String ipKey, String ipValue, String opKey, String opValue);

}

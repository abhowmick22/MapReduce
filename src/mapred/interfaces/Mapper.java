package mapred.interfaces;

/*
 * Interface for a mapper
 * It takes in a <String, String> pair for the KV input and emits a KV pair as the output
 * It is the job of the application programmer to provide appropriate conversions to/from
 * other types of KV pairs to <String, String> pair
 * Our recordreader will only read in strings from the files
 * 
 * Why do we need the generic type parameters ?
 */

public interface Mapper<X, Y, Z, W> {

}

package org.uu.lacpp15.g3.mapreduce.framework;

import java.util.Collection;

public interface MapReduceInput<K,V> {
	
	/**
	 * Retrieve a given number of concurrent iterators over the input. Every key-value pair in the
	 * input should be returned by exactly one of the iterators.
	 * 
	 * @param numIterators A number > 0 that specifies the number of iterators to return.
	 * @return A collection of concurrent iterators over the input.
	 */
	Collection<KeyValueIterator<K,V>> iterators(int numIterators);
	
}

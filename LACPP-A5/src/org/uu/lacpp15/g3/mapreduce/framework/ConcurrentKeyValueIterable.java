package org.uu.lacpp15.g3.mapreduce.framework;

import java.util.Collection;

public interface ConcurrentKeyValueIterable<K,V> {
	/**
	 * Retrieve a given number of concurrent iterators over the key-value pairs. Every key-value pair 
	 * should be returned by exactly one of the iterators.
	 * 
	 * @param numIterators A number > 0 that specifies the number of iterators to return.
	 * @return A collection of concurrent iterators over the key-value pairs.
	 */
	public Collection<KeyValueIterator<K,V>> iterators(int numIterators);
}

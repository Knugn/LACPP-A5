package org.uu.lacpp15.g3.mapreduce.framework;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

public class MapReduceInUtil {
	
	public static <K,V> MapReduceIn<K,V> fromConcurrentMap(final ConcurrentMap<K,V> map) {
		return new MapReduceIn<K,V>() {
			@Override
			public Collection<KeyValueIterator<K, V>> iterators(int numIterators) {
				return new ConcurrentMapIterable<>(map).iterators(numIterators);
			}
		};
	}
	
}

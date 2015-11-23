package org.uu.lacpp15.g3.mapreduce.framework;

public interface KeyValueIterable<K,V> {
	public KeyValueIterator<K,V> iterator();
}

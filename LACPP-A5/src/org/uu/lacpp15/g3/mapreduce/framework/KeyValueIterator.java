package org.uu.lacpp15.g3.mapreduce.framework;

public interface KeyValueIterator<K,V> {
	public boolean next();
	public K getKey();
	public V getValue();
}

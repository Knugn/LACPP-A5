package org.uu.lacpp15.g3.mapreduce.framework;

public interface KeyValueFormatter<K,V> {
	public String format(K key, V value);
}

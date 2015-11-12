package org.uu.lacpp15.g3.mapreduce.framework;

public interface KeyValueEmitter<K,V> {
	public void emit(K key, V value);
}

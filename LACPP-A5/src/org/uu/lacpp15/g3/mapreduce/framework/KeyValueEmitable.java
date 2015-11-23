package org.uu.lacpp15.g3.mapreduce.framework;

public interface KeyValueEmitable<K,V> {
	public KeyValueEmitter<K,V> emitter();
}

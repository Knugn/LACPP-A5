package org.uu.lacpp15.g3.mapreduce.framework;

public interface Reducer<K2,V2, K3,V3> {
	public void reduce(K2 key, Iterable<V2> values, KeyValueEmitter<K3, V3> emitter);
}

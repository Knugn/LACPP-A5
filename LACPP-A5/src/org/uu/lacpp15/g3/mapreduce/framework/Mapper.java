package org.uu.lacpp15.g3.mapreduce.framework;

public interface Mapper<K1,V1, K2,V2> {
	public void map(K1 key,V1 value, KeyValueEmitter<K2, V2> emitter);
}

package org.uu.lacpp15.g3.mapreduce.framework;

import java.util.List;

public interface Reducer<K2,V2,V3> {
	public void reduce(K2 key, List<V2> values, ValueEmitter<V3> emitter);
}

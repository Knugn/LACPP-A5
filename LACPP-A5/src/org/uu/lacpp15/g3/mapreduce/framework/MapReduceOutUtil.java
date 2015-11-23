package org.uu.lacpp15.g3.mapreduce.framework;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class MapReduceOutUtil {
	public static <K,V> MapReduceOut<K,V> toConcurrentMap(final ConcurrentMap<K,List<V>> map) {
		return new MapReduceOut<K, V>() {
			@Override
			public Collection<KeyValueEmitter<K, List<V>>> emitters(int numEmitters) {
				return new ConcurrentMapEmitable<K,List<V>>(map).emitters(numEmitters);
			}
		};
	}
}

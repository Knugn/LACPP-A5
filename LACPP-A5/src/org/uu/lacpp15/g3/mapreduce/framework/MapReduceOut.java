package org.uu.lacpp15.g3.mapreduce.framework;

import java.util.List;

public interface MapReduceOut<K,V> extends ConcurrentKeyValueEmitable<K,List<V>> {
	
}

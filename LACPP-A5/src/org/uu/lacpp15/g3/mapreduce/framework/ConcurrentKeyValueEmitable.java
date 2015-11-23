package org.uu.lacpp15.g3.mapreduce.framework;

import java.util.Collection;

public interface ConcurrentKeyValueEmitable<K,V> {
	public Collection<KeyValueEmitter<K,V>> emitters(int numEmitters);
}

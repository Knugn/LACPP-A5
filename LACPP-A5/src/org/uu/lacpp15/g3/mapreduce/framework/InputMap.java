package org.uu.lacpp15.g3.mapreduce.framework;

import java.util.List;
import java.util.Set;

public interface InputMap<K,V> {
	public interface Entry<K,V> {
		K getKey();
		V getValue();
	}
	Set<? extends Entry<K,V>> getEntrySet();
	Set<K> getKeySet();
	List<V> getValues();
	int count();
	List<? extends InputMap<K,V>> split(int nParts);
}

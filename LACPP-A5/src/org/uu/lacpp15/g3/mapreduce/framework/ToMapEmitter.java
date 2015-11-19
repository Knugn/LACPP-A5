package org.uu.lacpp15.g3.mapreduce.framework;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An emitter that stores emitted values for keys in a map. It is intended to be used by only one
 * thread at any time and is not thread-safe.
 * 
 * @author David
 * @param <K> Key type
 * @param <V> Value type
 */
public class ToMapEmitter<K, V> implements KeyValueEmitter<K, V> {
	
	Map<K, List<V>>	map;
	
	public ToMapEmitter(Map<K, List<V>> map) {
		setMap(map);
	}
	
	public Map<K, List<V>> getMap() {
		return map;
	}
	
	public void setMap(Map<K, List<V>> map) {
		this.map = map;
	}
	
	@Override
	public void emit(K key, V value) {
		if (!map.containsKey(key))
			map.put(key, new ArrayList<V>());
		List<V> list = map.get(key);
		list.add(value);
	}
	
}

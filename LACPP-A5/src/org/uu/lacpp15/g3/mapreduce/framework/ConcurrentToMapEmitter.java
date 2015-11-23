package org.uu.lacpp15.g3.mapreduce.framework;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class ConcurrentToMapEmitter <K,V> implements KeyValueEmitter<K,V> {
	
	private ConcurrentMap<K,List<V>> map;
	
	public ConcurrentToMapEmitter(ConcurrentMap<K, List<V>> map) {
		super();
		if (map == null)
			throw new IllegalArgumentException("map must not be null.");
		this.map = map;
	}

	public ConcurrentMap<K, List<V>> getMap() {
		return map;
	}

	@Override
	public void emit(K key, V value) {
		if (!map.containsKey(key))
			map.putIfAbsent(key, Collections.synchronizedList(new ArrayList<V>()));
		List<V> synclist = map.get(key);
		synclist.add(value);
	}
	
}

package org.uu.lacpp15.g3.mapreduce.framework;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

public class ConcurrentMapEmitable<K,V> implements ConcurrentKeyValueEmitable<K,V> {
	
	public static enum DuplicateKeyStrategy {
		Exception, Replace, Ignore
	}
	
	private ConcurrentMap<K,V> map;
	private DuplicateKeyStrategy duplicateKeyStrategy = DuplicateKeyStrategy.Exception;
	
	public ConcurrentMapEmitable(ConcurrentMap<K, V> map) {
		super();
		if (map == null)
			throw new IllegalArgumentException("map must not be null.");
		this.map = map;
	}
	
	public ConcurrentMap<K, V> getMap() {
		return map;
	}

	@Override
	public Collection<KeyValueEmitter<K, V>> emitters(int numEmitters) {
		ArrayList<KeyValueEmitter<K,V>> emitters = new ArrayList<>(numEmitters);
		for (int i=0; i<numEmitters; i++)
			emitters.add(new KeyValueEmitter<K,V>() {
				@Override
				public void emit(K key, V value) {
					switch (duplicateKeyStrategy) {
						case Exception:
							if (map.putIfAbsent(key, value) != null)
								throw new IllegalArgumentException("Key \""+key+"\" already exists the map.");
							break;
						case Replace:
							map.put(key, value);
						case Ignore:
							map.putIfAbsent(key, value);
					}
				}
			});
		return emitters;
	}
	
}

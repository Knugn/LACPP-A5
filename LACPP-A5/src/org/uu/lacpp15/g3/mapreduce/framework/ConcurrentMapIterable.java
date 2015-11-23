package org.uu.lacpp15.g3.mapreduce.framework;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

public class ConcurrentMapIterable<K,V> implements ConcurrentKeyValueIterable<K,V> {
	
	ConcurrentMap<K,V> map;
	
	public ConcurrentMapIterable(ConcurrentMap<K, V> map) {
		super();
		if (map == null)
			throw new IllegalArgumentException("map must not be null.");
		this.map = map;
	}

	@Override
	public Collection<KeyValueIterator<K, V>> iterators(int numIterators) {
		Iterator<Entry<K,V>> entries = map.entrySet().iterator();
		ArrayList<KeyValueIterator<K,V>> iterators = new ArrayList<>(numIterators);
		for (int i=0; i<numIterators; i++)
			iterators.add(new MapEntryIterator<>(entries));
		return iterators;
	}
	
}

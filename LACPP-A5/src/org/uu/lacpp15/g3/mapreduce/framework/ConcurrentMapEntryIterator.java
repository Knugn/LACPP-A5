package org.uu.lacpp15.g3.mapreduce.framework;

import java.util.Iterator;
import java.util.Map.Entry;

public class ConcurrentMapEntryIterator<K,V> implements KeyValueIterator<K,V> {
	
	Iterator<Entry<K,V>> iter;
	Entry<K,V> current = null;
	
	public ConcurrentMapEntryIterator(Iterator<Entry<K, V>> iter) {
		super();
		if (iter == null)
			throw new IllegalArgumentException("iter must not be null.");
		this.iter = iter;
	}

	@Override
	public boolean next() {
		synchronized (iter) {
			if (iter.hasNext()) {
				current = iter.next();
				return true;
			}
			return false;
		}
	}

	@Override
	public K getKey() {
		return current.getKey();
	}

	@Override
	public V getValue() {
		return current.getValue();
	}
	
}

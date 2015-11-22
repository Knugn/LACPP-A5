package org.uu.lacpp15.g3.mapreduce.framework;

public interface ValueEmitter<V> {
	public void emit(V value);
}

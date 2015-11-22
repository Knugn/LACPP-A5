package org.uu.lacpp15.g3.mapreduce.framework;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MapReduceEngine {
	Thread engineSupervisor;
	//ForkJoinPool mappers, reducers;
	ExecutorService mapExecutor, reduceExecutor;
	
	
	public MapReduceEngine(int nMapper, int nReducers) {
		//mappers = new ForkJoinPool(nMapper);
		//reducers = new ForkJoinPool(nReducers);
		mapExecutor = Executors.newFixedThreadPool(nMapper);
		reduceExecutor = Executors.newFixedThreadPool(nReducers);
	}
	
	public Future<Boolean> submitJob(MapReduceJob job) {
		//TODO
		return null;
	}
	
	public void runJob(MapReduceJob job) {
		//TODO
	}
	
	static class IntermediaryEmitter<K,V> implements KeyValueEmitter<K,V> {
		
		ConcurrentHashMap<K,List<V>> map;
		
		@Override
		public void emit(K key, V value) {
			if (!map.containsKey(key))
				map.putIfAbsent(key, Collections.synchronizedList(new ArrayList<V>()));
			List<V> synclist = map.get(key);
			synclist.add(value);
		}
		
		
	}
}

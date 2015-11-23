package org.uu.lacpp15.g3.mapreduce.framework;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MapReduceEngine {
	
	CompletionService<?> jobCompletionService; 
	int nMappers, nReducers;
	ExecutorService mapExecutor, reduceExecutor;
	
	public MapReduceEngine(int nMappers, int nReducers) {
		this.nMappers = nMappers;
		this.mapExecutor = Executors.newFixedThreadPool(nMappers);
		this.nReducers = nReducers;
		this.reduceExecutor = Executors.newFixedThreadPool(nReducers);
	}
	
	/**
	 * Submits a job to run asynchronously from the calling thread. Two use case scenarios when you
	 * would want to use this method are:
	 * <ul>
	 * <li>The calling thread wants to do some other work while the MapReduce job is processing.</li>
	 * <li>The engine is shared by multiple threads.</li>
	 * </ul>
	 * Be aware that the first time that this method is called on an engine, it will spawn a
	 * supervisor thread an can therefore have some additional overhead in the first call.
	 * 
	 * @param job The job to be run.
	 * @return A future representing the completion of the job. Calling {@link Future#get()} will
	 *         block and return null when the work has completed.
	 */
	public synchronized <K1,V1,K2,V2,V3> Future<?> submitJob(MapReduceJob<K1,V1,K2,V2,V3> job) {
		if (jobCompletionService == null)
			this.jobCompletionService = new ExecutorCompletionService<>(Executors.newSingleThreadExecutor());
		return jobCompletionService.submit(new RunnableMapReduceJob<>(job), null);
	}
	
	/**
	 * Runs a job synchronously by using the calling thread as a supervisor of the job. Use this
	 * method to run jobs if the engine is used by a single thread that doesn't need/wish to do
	 * something else while the job is running.
	 * 
	 * @param job The job to be run.
	 */
	public <K1,V1,K2,V2,V3> void runJob(MapReduceJob<K1,V1,K2,V2,V3> job) {
		new RunnableMapReduceJob<>(job).run();
	}
	
	public class RunnableMapReduceJob<K1,V1,K2,V2,V3> implements Runnable {
		
		MapReduceJob<K1,V1,K2,V2,V3> job;
		
		public RunnableMapReduceJob(MapReduceJob<K1,V1,K2,V2,V3> job) {
			super();
			if (job == null)
				throw new IllegalArgumentException("job must not be null.");
			this.job = job;
		}

		@Override
		public void run() {
			ConcurrentMap<K2,List<V2>> intermediaryMap = runMappers();
			ConcurrentKeyValueIterable<K2,List<V2>> intermediaryIterable = new ConcurrentMapIterable<>(intermediaryMap);
			runReducers(intermediaryIterable);
		}
		
		private ConcurrentMap<K2,List<V2>> runMappers() {
			ToMultivaluedConcurrentMapEmitter<K2,V2> emitter = new ToMultivaluedConcurrentMapEmitter<>(new ConcurrentHashMap<K2,List<V2>>());
			CompletionService<Map<K2,List<V2>>> mapperCompletionService = new ExecutorCompletionService<>(mapExecutor);
			for (KeyValueIterator<K1,V1> iter : job.getIn().iterators(nMappers)) {
				mapperCompletionService.submit(new RunnableMapper(iter, emitter), null);
			}
			for (int i=0; i < nMappers; i++) {
				try {
					Future<Map<K2,List<V2>>> mapperFuture = mapperCompletionService.take();
					mapperFuture.get();
				}
				catch (InterruptedException e) {
					// TODO Better exception handling.
					System.err.println("A MapReduce job was interupted while waiting for mappers to complete.");
					e.printStackTrace();
				}
				catch (ExecutionException e) {
					// TODO Better exception handling.
					System.err.println("A mapper of a MapReduce job threw an exception.");
					e.printStackTrace();
				}
			}
			return emitter.getMap();
		}
		
		private void runReducers(ConcurrentKeyValueIterable<K2, List<V2>> intermediaryIterable) {
			Iterator<KeyValueEmitter<K2,List<V3>>> reducerEmitters = job.getOut().emitters(nReducers).iterator();
			CompletionService<Map<K2,List<V2>>> reducerCompletionService = new ExecutorCompletionService<>(reduceExecutor);
			for (KeyValueIterator<K2,List<V2>> iter : intermediaryIterable.iterators(nReducers)) {
				reducerCompletionService.submit(new RunnableReducer(iter, reducerEmitters.next()), null);
			}
			for (int i=0; i < nReducers; i++) {
				try {
					Future<Map<K2,List<V2>>> reducerFuture = reducerCompletionService.take();
					reducerFuture.get();
				}
				catch (InterruptedException e) {
					// TODO Better exception handling.
					System.err.println("A MapReduce job was interupted while waiting for reducers to complete.");
					e.printStackTrace();
				}
				catch (ExecutionException e) {
					// TODO Better exception handling.
					System.err.println("A reducer of a MapReduce job threw an exception.");
					e.printStackTrace();
				}
			}
		}
		
		private class RunnableMapper implements Runnable {

			KeyValueIterator<K1,V1> iter;
			KeyValueEmitter<K2,V2> emitter;
			
			public RunnableMapper(KeyValueIterator<K1, V1> iter, KeyValueEmitter<K2,V2> emitter) {
				super();
				this.iter = iter;
				this.emitter = emitter;
			}

			@Override
			public void run() {
				Mapper<K1,V1,K2,V2> mapper = job.getMapper();
				while (iter.next()) {
					mapper.map(iter.getKey(), iter.getValue(), emitter);
				}
			}
			
		}
		
		private class RunnableReducer implements Runnable {
			
			KeyValueIterator<K2,List<V2>> iter;
			KeyValueEmitter<K2,List<V3>> emitter;
			
			public RunnableReducer(KeyValueIterator<K2, List<V2>> iter, KeyValueEmitter<K2,List<V3>> emitter) {
				super();
				this.iter = iter;
				this.emitter = emitter;
			}
			
			@Override
			public void run() {
				Reducer<K2,V2,V3> reducer = job.getReducer();
				while (iter.next()) {
					ToListEmitter<V3> innerEmitter = new ToListEmitter<>(new LinkedList<V3>());
					K2 key = iter.getKey();
					reducer.reduce(key, iter.getValue(), innerEmitter);
					emitter.emit(key, innerEmitter.getList());
				}
			}
			
		}
		
	}
}

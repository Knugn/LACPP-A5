package org.uu.lacpp15.g3.mapreduce.framework;

/**
 * A {@link MapReduceJob} contains a description of a job that can be executed by {@link MapReduceEngine}.
 * @author David
 *
 * @param <K1> Type of the input keys.
 * @param <V1> Type of the input values.
 * @param <K2> Type of the output keys.
 * @param <V2> Type of intermediary values (sent from map to reduce).
 * @param <V3> Type of the output values.
 */
public class MapReduceJob<K1,V1,K2,V2,V3> {
	
	private MapReduceIn<K1,V1> in;
	private Mapper<K1,V1,K2,V2> mapper;
	private Reducer<K2,V2,V3> reducer;
	private MapReduceOut<K2,V3> out;
	
	public MapReduceJob(
			MapReduceIn<K1,V1> in,
			Mapper<K1,V1,K2,V2> mapper,
			Reducer<K2,V2,V3> reducer,
			MapReduceOut<K2,V3> out) {
		this.in = in;
		this.mapper = mapper;
		this.reducer = reducer;
		this.out = out;
	}

	public MapReduceIn<K1, V1> getIn() {
		return in;
	}

	public Mapper<K1, V1, K2, V2> getMapper() {
		return mapper;
	}

	public Reducer<K2, V2, V3> getReducer() {
		return reducer;
	}

	public MapReduceOut<K2, V3> getOut() {
		return out;
	}
	
}

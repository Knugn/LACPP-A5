package org.uu.lacpp15.g3.mapreduce.framework;

public class MapReduceJob<K1,V1, K2,V2, K3,V3> {
	KeyValueIterator<K1,V1> inputIterator;
	Mapper<K1,V1, K2,V2> mapper;
	Reducer<K2,V2, K3,V3> reducer;
	KeyValueEmitter<K3,V3> outputEmitter;
	
	public MapReduceJob(
			KeyValueIterator<K1,V1> inputIterator,
			Mapper<K1,V1, K2,V2> mapper,
			Reducer<K2,V2, K3,V3> reducer,
			KeyValueEmitter<K3,V3> outputEmitter) {
		this.inputIterator = inputIterator;
		this.mapper = mapper;
		this.reducer = reducer;
		this.outputEmitter = outputEmitter;
	}
	
}

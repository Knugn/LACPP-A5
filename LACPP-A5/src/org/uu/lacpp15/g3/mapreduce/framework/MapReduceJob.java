package org.uu.lacpp15.g3.mapreduce.framework;

public class MapReduceJob<K1,V1,K2,V2,V3> {
	MapReduceInput<K1,V1> input;
	Mapper<K1,V1,K2,V2> mapper;
	Reducer<K2,V2,V3> reducer;
	KeyValueEmitter<K2,V3> outputEmitter;
	
	public MapReduceJob(
			MapReduceInput<K1,V1> input,
			Mapper<K1,V1,K2,V2> mapper,
			Reducer<K2,V2,V3> reducer,
			KeyValueEmitter<K2,V3> outputEmitter) {
		this.input = input;
		this.mapper = mapper;
		this.reducer = reducer;
		this.outputEmitter = outputEmitter;
	}
	
}

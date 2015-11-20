package org.uu.lacpp15.g3.mapreduce.implementations;

import org.uu.lacpp15.g3.mapreduce.framework.KeyValueEmitter;
import org.uu.lacpp15.g3.mapreduce.framework.Mapper;
import org.uu.lacpp15.g3.mapreduce.framework.Reducer;

public class WordCount {

	
	public static void main(String[] args) {
		
	}
	

	public class WordCountMapper implements Mapper<String, String, String, Integer>{

		@Override
		public void map(String key, String value,
				KeyValueEmitter<String, Integer> emitter) {
			emitter.emit(value.toLowerCase(), 1);
		}
		
		
	}

	
	public class WordCountReducer implements Reducer<String, Integer, String, Integer>
	{
		@Override
		public void reduce(String key, Iterable<Integer> values,
				KeyValueEmitter<String, Integer> emitter) {
			// TODO Auto-generated method stub
		int sum = 0;
			for(Integer value: values){
				sum =+ value;
			}
			emitter.emit(key, sum);
		}
	}
}

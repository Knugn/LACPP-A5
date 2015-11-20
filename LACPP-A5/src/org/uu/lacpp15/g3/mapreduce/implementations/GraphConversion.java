package org.uu.lacpp15.g3.mapreduce.implementations;

import org.uu.lacpp15.g3.mapreduce.framework.KeyValueEmitter;
import org.uu.lacpp15.g3.mapreduce.framework.Mapper;
import org.uu.lacpp15.g3.mapreduce.framework.Reducer;

public class GraphConversion {

	
	public static void main(String[] args) {
		
	}
	

	public class GraphConversionMapper implements Mapper<String, String, Integer, Integer>{

		@Override
		public void map(String key, String value,
				KeyValueEmitter<Integer, Integer> emitter) {
			
			value.replaceAll("[^0-9]+", " ");
			String[] values = value.split(" ");
			int value1 = Integer.parseInt(values[0]);
			int value2 = Integer.parseInt(values[1]);
			
			emitter.emit(value1, value2);
			emitter.emit(value2, value1);
		}
		
		
	}

	
	public class GraphConversionReducer implements Reducer<Integer, Integer, Integer, String>
	{
		@Override
		public void reduce(Integer key, Iterable<Integer> values,
				KeyValueEmitter<Integer, String> emitter) {
			// TODO Auto-generated method stub
		String neighbor = key + " # ";
			for(Integer value: values){
				neighbor += " " + value;
			}
			emitter.emit(key, neighbor);
		}
	}
}

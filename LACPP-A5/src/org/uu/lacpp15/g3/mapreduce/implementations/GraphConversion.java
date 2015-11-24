package org.uu.lacpp15.g3.mapreduce.implementations;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.uu.lacpp15.g3.mapreduce.framework.KeyValueEmitter;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceEngine;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceIn;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceInUtil;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceJob;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceOutUtil;
import org.uu.lacpp15.g3.mapreduce.framework.Mapper;
import org.uu.lacpp15.g3.mapreduce.framework.Reducer;
import org.uu.lacpp15.g3.mapreduce.framework.ValueEmitter;

public class GraphConversion {



	public static Map<Integer, List<String>> run(String text,int threds){
		
		ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
		
		String[] inputText = text.split("\\n+");
		int keyNr = 0;
		for (String string : inputText) {
			map.put("" + keyNr++, string);
		}
				
		
		return run(MapReduceInUtil.fromConcurrentMap(map), threds);
	}


public static Map<Integer, List<String>> run(MapReduceIn<String, String> map,int threds){
		

		ConcurrentHashMap<Integer, List<String>> outMap = new ConcurrentHashMap<>();
		
		
		MapReduceJob<String, String, Integer, Integer, String> job = new MapReduceJob<>(
				map, 
				new GraphConversionMapper(),
				new GraphConversionReducer(),
				MapReduceOutUtil.toConcurrentMap(outMap));
		MapReduceEngine engine = new MapReduceEngine(threds, threds);
		engine.runJob(job);
		return outMap;
	}

	
	public static class GraphConversionMapper implements Mapper<String, String, Integer, Integer>{

		@Override
		public void map(String key, String value,
				KeyValueEmitter<Integer, Integer> emitter) {

			value = value.replaceAll("[(|)]", "");
			String[] values = value.split(",");
			int value1 = Integer.parseInt(values[0]);
			int value2 = Integer.parseInt(values[1]);

			emitter.emit(value1, value2);
			emitter.emit(value2, value1);
		}


	}


	public static class GraphConversionReducer implements Reducer<Integer, Integer, String>
	{
		@Override
		public void reduce(Integer key, List<Integer> values,
				ValueEmitter<String> emitter) {

			String neighbor = key + " #";
			for(Integer value: values){
				neighbor += " " + value;
			}
			emitter.emit(neighbor);
		}
	}
}

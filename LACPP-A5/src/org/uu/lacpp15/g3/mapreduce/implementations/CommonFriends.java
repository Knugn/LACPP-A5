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

public class CommonFriends {

	public static Map<String, List<String>> run(String text, int threads){
		ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();

		Map<Integer,List<String>> mapNegbours = GraphConversion.run(text,threads);
		//System.out.println(mapNegbours);
		for (Map.Entry<Integer,List<String>> entry : mapNegbours.entrySet()) {
			map.put(entry.getKey().toString(), entry.getValue().get(0));
		}


		ConcurrentHashMap<String, List<String>> outMap = new ConcurrentHashMap<>();


		MapReduceJob<String, String, String, Integer, String> job = new MapReduceJob<>(
				MapReduceInUtil.fromConcurrentMap(map), 
				new GraphConversionMapper(),
				new GraphConversionReducer(),
				MapReduceOutUtil.toConcurrentMap(outMap));
		MapReduceEngine engine = new MapReduceEngine(threads, threads);
		engine.runJob(job);
		return outMap;
	}
	
	public static Map<String, List<String>> run(MapReduceIn<String, String> inputMap, int threads){

		Map<Integer,List<String>> mapNegbours = GraphConversion.run(inputMap,threads);
		//System.out.println(mapNegbours);
		ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
		for (Map.Entry<Integer,List<String>> entry : mapNegbours.entrySet()) {
			map.put(entry.getKey().toString(), entry.getValue().get(0));
		}


		ConcurrentHashMap<String, List<String>> outMap = new ConcurrentHashMap<>();


		MapReduceJob<String, String, String, Integer, String> job = new MapReduceJob<>(
				MapReduceInUtil.fromConcurrentMap(map), 
				new GraphConversionMapper(),
				new GraphConversionReducer(),
				MapReduceOutUtil.toConcurrentMap(outMap));
		MapReduceEngine engine = new MapReduceEngine(threads, threads);
		engine.runJob(job);
		return outMap;
	}
	


	public static class GraphConversionMapper implements Mapper<String, String, String, Integer>{

		@Override
		public void map(String key, String value,
				KeyValueEmitter<String, Integer> emitter) {

			//input is A person han all his/her friends
			value = value.replaceAll("# ", "");
			String[] split = value.split(" ");
		
			int self = Integer.parseInt(split[0]);
			for(int x = 1; x < split.length; x++){
				for(int y = x+1; y < split.length; y++){
					String outKey = "";
					if (x < y){
						outKey = split[x] + " " +  split[y];
					}
					else{
						outKey = split[y] + " " +  split[x];
					}
					emitter.emit(outKey, self);
				}
			}					
			
		}
	}		



	public static class GraphConversionReducer implements Reducer<String, Integer, String>
	{
		@Override
		public void reduce(String key, List<Integer> values,
				ValueEmitter<String> emitter) {

			String ans = key + " #";
			for (Integer value : values) {
				ans += " "  + value;
			}			
			emitter.emit(ans);

		}

	}
}
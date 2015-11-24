package org.uu.lacpp15.g3.mapreduce.implementations;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.uu.lacpp15.g3.mapreduce.framework.KeyValueEmitter;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceEngine;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceInUtil;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceJob;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceOutUtil;
import org.uu.lacpp15.g3.mapreduce.framework.Mapper;
import org.uu.lacpp15.g3.mapreduce.framework.Reducer;
import org.uu.lacpp15.g3.mapreduce.framework.ValueEmitter;
import org.uu.lacpp15.g3.mapreduce.implementations.WordCount.WordCountMapper;
import org.uu.lacpp15.g3.mapreduce.implementations.WordCount.WordCountReducer;

public class CommonFriends {
	
	public static Map<String, List<String>> run(String text, int n){
		ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
		map.put("file1", text);

		ConcurrentHashMap<String, List<String>> outMap = new ConcurrentHashMap<>();
		
		
		MapReduceJob<String, String, String, Integer, String> job = new MapReduceJob<>(
				MapReduceInUtil.fromConcurrentMap(map), 
				new GraphConversionMapper(n),
				new GraphConversionReducer(),
				MapReduceOutUtil.toConcurrentMap(outMap));
		MapReduceEngine engine = new MapReduceEngine(4, 4);
		engine.runJob(job);
		return outMap;
	}
	

	public static class GraphConversionMapper implements Mapper<String, String, String, Integer>{
		int n;

		GraphConversionMapper(int n){
			this.n = n;
		}

		@Override
		public void map(String key, String value,
				KeyValueEmitter<String, Integer> emitter) {
			
			//input is A person han all his/her friends
			value = value.replaceAll("[(|)]", "");
			String[] values = value.split(" ");
			int value1 = Integer.parseInt(values[0]);
			int value2 = Integer.parseInt(values[1]);
			//send 
			for (int i = 1;  i < n + 1; i++){
				if (value1 != i && value2 != i){
					String outKey = "";
					if (value1 < i){
						outKey = i + " " + value1;
					}
					else{
						outKey = value1 + " " + i;
					}
					emitter.emit(outKey, value2);

					outKey = "";
					if (value2 < i){
						outKey = i + " " + value2;
					}
					else{
						outKey = value2 + " " + i;
					}
					emitter.emit(outKey, value1);
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
			Collections.sort(values);
			int curentValue = -1;
			for (Integer value : values) {
				if (curentValue == value){
					ans += " " + curentValue;	
					curentValue = -1;	
				}else{	
					curentValue = value;
				}
			}
			emitter.emit(ans);

		}

	}
}
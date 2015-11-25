package org.uu.lacpp15.g3.mapreduce.implementations;

import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.uu.lacpp15.g3.mapreduce.framework.KeyValueEmitter;
import org.uu.lacpp15.g3.mapreduce.framework.KeyValueFormatter;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceEngine;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceIn;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceInUtil;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceJob;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceOut;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceOutUtil;
import org.uu.lacpp15.g3.mapreduce.framework.Mapper;
import org.uu.lacpp15.g3.mapreduce.framework.PathGenerator;
import org.uu.lacpp15.g3.mapreduce.framework.Reducer;
import org.uu.lacpp15.g3.mapreduce.framework.ValueEmitter;

public class WordCount {

	
	public static void main(String[] args) throws FileNotFoundException, URISyntaxException {
		run(args);

	}
	
	public static void run(final String[] args) throws FileNotFoundException, URISyntaxException {
		String filePath = args[0];
		int mapper = 1;
		int reducers = 1;
		if (args.length > 3){
			mapper = Integer.parseInt(args[2]);
			reducers = Integer.parseInt(args[3]);
		}
		List<URI> inputFIle = new ArrayList<URI>();
		Path path2 = Paths.get(filePath);
		inputFIle.add(path2.toUri());

		MapReduceOut<String,Integer> output = MapReduceOutUtil.toFiles(new PathGenerator() {
			
			@Override
			public Path next() {
			
				return Paths.get(args[1],"/WordCount");
			}
		}, new KeyValueFormatter<String, List<Integer>>() {

			@Override
			public String format(String key, List<Integer> value) {
				return key + " " + value.get(0);
			}
		},0,null);

		WordCount.run(MapReduceInUtil.fromFileLines(inputFIle),mapper,reducers,output);
		//PrintWriter out = new PrintWriter(args[1]);


	}
	
	public static Map<String, List<Integer>> run(String text,int mappers, int reducers){
		ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
		map.put("file1", text);
	
	ConcurrentHashMap<String, List<Integer>> outMap = new ConcurrentHashMap<String, List<Integer>>();
		
		
		MapReduceJob<String, String, String, Integer, Integer> job = new MapReduceJob<>(
				MapReduceInUtil.fromConcurrentMap(map), 
				new WordCountMapper(),
				new WordCountReducer(),
				MapReduceOutUtil.toConcurrentMap(outMap));
		MapReduceEngine engine = new MapReduceEngine(mappers, reducers);
		engine.runJob(job);
		engine.close();
		return outMap;
	}
	

	public static void run(MapReduceIn<String, String> map,int mappers, int reducers,MapReduceOut<String,Integer> output){

		
		
		MapReduceJob<String, String, String, Integer, Integer> job = new MapReduceJob<>(
				map, 
				new WordCountMapper(),
				new WordCountReducer(),
				output);
		MapReduceEngine engine = new MapReduceEngine(mappers, reducers);
		engine.runJob(job);
		engine.close();
	}
	
	public static class WordCountMapper implements Mapper<String, String, String, Integer>{

		@Override
		public void map(String key, String value,
				KeyValueEmitter<String, Integer> emitter) {
			String[] values = value.split("\\s+");
			for (String string : values) {
				emitter.emit(string.toLowerCase(), 1);
			}
			
		}
	}

	
	public static class WordCountReducer implements Reducer<String, Integer, Integer>
	{
		@Override
		public void reduce(String key, List<Integer> values,
				ValueEmitter<Integer> emitter) {

			int sum = 0;
			for(Integer value: values){
				sum += value;
			}
			emitter.emit(sum);
		}
	}
}

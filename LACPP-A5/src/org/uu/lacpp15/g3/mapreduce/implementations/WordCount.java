package org.uu.lacpp15.g3.mapreduce.implementations;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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

public class WordCount {

	
	public static void main(String[] args) throws FileNotFoundException, URISyntaxException {
		String filePath = args[0];
		List<URI> inputFIle = new ArrayList<URI>();
		Path path2 = Paths.get(filePath);
		inputFIle.add(new URI(filePath));
		
		Map<String,List<Integer>> map = WordCount.run(MapReduceInUtil.fromFileLines(inputFIle),10);
		PrintWriter out = new PrintWriter("wordCount.txt");
		out.print(map.toString());
		out.close();
	}
	
	public static Map<String, List<Integer>> run(String text,int threads){
		ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
		map.put("file1", text);
		return run(MapReduceInUtil.fromConcurrentMap(map),threads);
		
	}
	

	public static Map<String, List<Integer>> run(MapReduceIn<String, String> map,int threads){

		ConcurrentHashMap<String, List<Integer>> outMap = new ConcurrentHashMap<String, List<Integer>>();
		
		
		MapReduceJob<String, String, String, Integer, Integer> job = new MapReduceJob<>(
				map, 
				new WordCountMapper(),
				new WordCountReducer(),
				MapReduceOutUtil.toConcurrentMap(outMap));
		MapReduceEngine engine = new MapReduceEngine(threads, threads);
		engine.runJob(job);
		return outMap;
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

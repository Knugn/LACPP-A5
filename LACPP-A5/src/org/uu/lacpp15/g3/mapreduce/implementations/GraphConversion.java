package org.uu.lacpp15.g3.mapreduce.implementations;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.net.URI;
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
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceOutUtil;
import org.uu.lacpp15.g3.mapreduce.framework.Mapper;
import org.uu.lacpp15.g3.mapreduce.framework.PathGenerator;
import org.uu.lacpp15.g3.mapreduce.framework.Reducer;
import org.uu.lacpp15.g3.mapreduce.framework.ValueEmitter;

public class GraphConversion {

	public static void main(String[] args) throws FileNotFoundException {
		run(args, System.out);
		System.exit(0);
	}
	
	public static void run(final String[] args,PrintStream out) throws FileNotFoundException {

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

		Map<Integer, List<String>> map = GraphConversion.run(MapReduceInUtil.fromFileLines(inputFIle), mapper,reducers);
		//PrintWriter out = new PrintWriter("Conversion.txt");
		MapReduceOutUtil.toFiles(new PathGenerator() {
			
			@Override
			public Path next() {
			
				return Paths.get(args[1],"/commonFriends");
			}
		}, new KeyValueFormatter<String, List<String>>() {

			@Override
			public String format(String key, List<String> value) {
				return value.get(0);
			}
		},0,null);
		out.print(map.toString());
		out.close();
	}

	public static Map<Integer, List<String>> run(String text,int mapper,int reducers){

		ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();

		String[] inputText = text.split("\\n+");
		int keyNr = 0;
		for (String string : inputText) {
			map.put("" + keyNr++, string);
		}


		return run(MapReduceInUtil.fromConcurrentMap(map), mapper,reducers);
	}


	public static Map<Integer, List<String>> run(MapReduceIn<String, String> map,int mapper,int reducers){


		ConcurrentHashMap<Integer, List<String>> outMap = new ConcurrentHashMap<>();


		MapReduceJob<String, String, Integer, Integer, String> job = new MapReduceJob<>(
				map, 
				new GraphConversionMapper(),
				new GraphConversionReducer(),
				MapReduceOutUtil.toConcurrentMap(outMap));
		MapReduceEngine engine = new MapReduceEngine(mapper, reducers);
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

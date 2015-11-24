package org.uu.lacpp15.g3.mapreduce.implementations;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URI;
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

public class CommonFriends {



	public static void main(String[] args) throws FileNotFoundException {
		run(args,System.out);
		System.exit(0);
	}
	
	public static void run(String[] args,PrintStream out) throws FileNotFoundException {
		String filePath = args[0];
		int mapper = 1;
		int reducers = 1;
		if (args.length > 3){
			mapper = Integer.parseInt(args[3]);
			reducers = Integer.parseInt(args[4]);
		}
		List<URI> inputFIle = new ArrayList<URI>();
		Path path2 = Paths.get(filePath);
		inputFIle.add(path2.toUri());

		Map<String, List<String>> map = CommonFriends.run(MapReduceInUtil.fromFileLines(inputFIle), mapper,reducers);
		//PrintWriter out = new PrintWriter("commonFriends.txt");
		out.print(map.toString());
		out.close();
	}


	public static Map<String, List<String>> run(String text, int mapper,int reducers){
		ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();

		Map<Integer,List<String>> mapNegbours = GraphConversion.run(text,mapper,reducers);
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
		MapReduceEngine engine = new MapReduceEngine(mapper, reducers);
		engine.runJob(job);
		return outMap;
	}

	public static Map<String, List<String>> run(MapReduceIn<String, String> inputMap, int mapper, int reducer){

		Map<Integer,List<String>> mapNegbours = GraphConversion.run(inputMap,mapper,reducer);
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
		MapReduceEngine engine = new MapReduceEngine(mapper, reducer);
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
					if (split[x].compareTo(split[y]) < 0){
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
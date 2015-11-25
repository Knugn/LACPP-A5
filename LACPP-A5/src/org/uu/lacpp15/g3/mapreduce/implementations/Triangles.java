package org.uu.lacpp15.g3.mapreduce.implementations;

import java.io.FileNotFoundException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
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

public class Triangles {

	
	
	public static void main(String[] args) throws FileNotFoundException {
		run(args);
	//	System.exit(0);
	}


	public static void run(final String[] args) throws FileNotFoundException {
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
		MapReduceOut<Integer , Integer> output = MapReduceOutUtil.toFiles(new PathGenerator() {
			
			@Override
			public Path next() {
			
				return Paths.get(args[1],"/Triangels.txt");
			}
		}, new KeyValueFormatter<Integer, List<Integer>>() {

			@Override
			public String format(Integer key, List<Integer> value) {
				return key + " " + value.get(0);
			}
		},0,null);


		Triangles.run(MapReduceInUtil.fromFileLines(inputFIle),mapper,reducers,output);
		//PrintWriter out = new PrintWriter("Triangles.txt");	
	}
	
	public static void run(MapReduceIn<String, String> inputMap, int mapper,int reducers, MapReduceOut<Integer , Integer> output){


		ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();	
		Map<Integer,List<String>> mapNegbours = GraphConversion.run(inputMap,mapper,reducers);



		for (Map.Entry<Integer,List<String>> entry : mapNegbours.entrySet()) {
			map.put(entry.getKey().toString(), entry.getValue().get(0));
		}


		MapReduceJob<String, String, Integer, int[], Integer> job = new MapReduceJob<>(
				MapReduceInUtil.fromConcurrentMap(map), 
				new GraphTrianglesMapper(),
				new GraphTrianglesReducer(),
				output);
		MapReduceEngine engine = new MapReduceEngine(mapper,reducers);
		engine.runJob(job);
		engine.close();
	}

	public static Map<Integer, List<Integer>> run(String text, int mapper,int reducers){


		ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
		Map<Integer,List<String>> mapNegbours = GraphConversion.run(text,mapper,reducers);
		for (Map.Entry<Integer,List<String>> entry : mapNegbours.entrySet()) {
			map.put(entry.getKey().toString(), entry.getValue().get(0));
		}

		ConcurrentHashMap<Integer, List<Integer>> outMap = new ConcurrentHashMap<>();


		MapReduceJob<String, String, Integer, int[], Integer> job = new MapReduceJob<>(
				MapReduceInUtil.fromConcurrentMap(map), 
				new GraphTrianglesMapper(),
				new GraphTrianglesReducer(),
				MapReduceOutUtil.toConcurrentMap(outMap));
		MapReduceEngine engine = new MapReduceEngine(mapper,reducers);
		engine.runJob(job);
		engine.close();
		return outMap;
	}

	public static class GraphTrianglesMapper implements Mapper<String, String, Integer, int[]>{

		@Override
		public void map(String key, String value,
				KeyValueEmitter<Integer, int[]> emitter) {
			//input is A person and B all his/her friends
			value = value.replaceAll("# ", "");
			String[] split = value.split(" ");
			int[] values = new int[split.length];
			values[0] = -1;
			for(int i = 1; i < split.length; i++){
				values[i] = Integer.parseInt(split[i]);
			}
			Arrays.sort(values);
			values[0] = Integer.parseInt(split[0]);
			for(int outKey: values){
				emitter.emit(outKey, values);
			}
		}	

	}

	public static class GraphTrianglesReducer implements Reducer<Integer, int[], Integer>
	{
		@Override
		public void reduce(Integer key, List<int[]> values,
				ValueEmitter<Integer> emitter) {

			int[] selfFriends = null;
			for (int[] value: values){
				if (key == value[0]){
					selfFriends = value;
					break;
				}
			}
			int ans = 0;
			for (int[] friends: values){
				if (key == friends[0]){
					continue;
				}
				int counter = 1;
				int counter2 = 1;
				//Compare all negbours with all negbours of negbours
				while(counter2 < friends.length && counter < selfFriends.length){
					if (selfFriends[counter] == friends[counter2]){
						ans++;
						counter2++;
						counter++;
					}else if(selfFriends[counter] > friends[counter2]){
						counter2++;
					}else{
						counter++;
					}
				}
			}
			//Bas solution calculated every ting 2 times
			emitter.emit(ans/2);
		}

	}
}

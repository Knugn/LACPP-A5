package org.uu.lacpp15.g3.mapreduce.implementations;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceInUtil;

public class WordCountTest {

	
	
/*	
	@Test
	public void testFile() {
		List<URI> inputFIle = new ArrayList<URI>();
		Path path2 = Paths.get("src/org/uu/lacpp15/g3/mapreduce/resoucre/book.txt");
		//System.out.println(path2.toAbsolutePath().toString());
		inputFIle.add(path2.toUri());
		//String test = GraphGen.out(1000,10);r
		Map<String,List<Integer>> map = WordCount.run(MapReduceInUtil.fromFileLines(inputFIle),10);
		System.out.println(map.toString());
	}
	*/
	@Test
	public void test() {
		//4 test 1 test2 2 test3 1 test4
 		String text = "test test test2 test3 test test3 test test4";
 		Map<String, List<Integer>> map = WordCount.run(text,4);
 	//	System.out.print(map.toString());
 		TreeMap<String,List<Integer>> tree = new TreeMap<>(map);
 		System.out.print(tree);
 		int[] expectedValues = {4,1,2,1};
 		int counter = 0;
 		for(List<Integer> value: tree.values()){
 			assert(value.size() == 1);
 			assert(value.get(0) == expectedValues[counter]);
 			counter++;
 		}
	}

}

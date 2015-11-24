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

public class TrianglesTest {

	
	
	@Test
	public void test() {
		String test = "(1,3)\n(2,3)\n(2,5)\n(3,5)\n(3,4)\n(2,4)";
		//String test = GraphGen.out(1000,10);r
		Map<Integer,List<Integer>> map = Triangles.run(test,2);
		//System.out.print(map);
		
		TreeMap<Integer, List<Integer>> tree = new TreeMap<>(map);
		System.out.println(tree);
		int[] expectedValues = {0,2,2,1,1};
		int[] expectedKey = {1,2,3,4,5};
		int counter = 0;

		for(Map.Entry<Integer, List<Integer>> value: tree.entrySet()){
			assert(value.getValue().size() == 1);

			assert(value.getValue().get(0) == expectedValues[counter]);
			assert(value.getKey() == expectedKey[counter]);
			counter++;
		}
		
	}

}

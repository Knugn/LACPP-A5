package org.uu.lacpp15.g3.mapreduce.implementations;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

public class WordCountTest {

	@Test
	public void test() {
		//4 test 1 test2 2 test3 1 test4
 		String text = "test test test2 test3 test test3 test test4";
 		Map<String, List<Integer>> map = WordCount.run(text);
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

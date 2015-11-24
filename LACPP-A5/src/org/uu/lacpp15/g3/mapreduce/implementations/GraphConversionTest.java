package org.uu.lacpp15.g3.mapreduce.implementations;


import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.uu.lacpp15.g3.mapreduce.exercises.GraphGen;

public class GraphConversionTest {

	@Test
	public void test() {
		//1 # 2 3
		//2 # 1
		//3 # 1
		//4 # 5
		//5 # 4
		String test = "(1,2)\n(3,1)\n(4,5)";
		//String test = GraphGen.out(1000,10);r
		Map<Integer,List<String>> map = GraphConversion.run(test);
		System.out.print(map);
	}

}

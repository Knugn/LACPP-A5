package org.uu.lacpp15.g3.mapreduce.implementations;

import java.util.List;
import java.util.Map;

import org.junit.Test;

public class TrianglesTest {

	@Test
	public void test() {
		String test = "(1,3)\n(2,3)\n(2,5)\n(3,5)";
		//String test = GraphGen.out(1000,10);r
		Map<Integer,List<Integer>> map = Triangles.run(test,2);
		System.out.print(map);
	}

}

package org.uu.lacpp15.g3.mapreduce.implementations;


import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class GraphConversionTest {

	

	
	//@Test
	public static void test() {
		//1 # 2 3
		//2 # 1
		//3 # 1
		//4 # 5
		//5 # 4
		String test = "(1,3)\n(2,3)\n(2,5)\n(3,5)";
		Map<Integer,List<String>> map = GraphConversion.run(test,10,10);
		System.out.println(map);
		TreeMap<Integer, List<String>> tree = new TreeMap<>(map);
		int[][] expectedValues = {{1,3},{2,3,5},{3,1,2,5},{5,2,3}};
		int counter = 0;

		for(List<String> value: tree.values()){
			assert(value.size() == 1);
			String str = value.get(0).replaceAll("# ", "");
			//System.out.println(str);
			
			String[] split = str.split(" ");
			String temp = split[0];
			split[0] = "-1";
			Arrays.sort(split);
			split[0] = temp;
			for (int i = 0; i < split.length; i++){
				System.out.println(split[i] + " " + expectedValues[counter][i]);
				assert(Integer.parseInt(split[i]) == expectedValues[counter][i]);
			}
			counter++;
		}
	}

}

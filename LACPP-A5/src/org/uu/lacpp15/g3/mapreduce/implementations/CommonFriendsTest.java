package org.uu.lacpp15.g3.mapreduce.implementations;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CommonFriendsTest {

//	@Test
	public void test() {
		String test = "(1,3)\n(2,5)\n(3,5)\n(3,4)\n(2,4)";
		
		//Map<String,List<String>> map = CommonFriends.run(test,10);
		//System.out.print(map);
		Map<String,List<String>> map = CommonFriends.run(test,10);
		
		TreeMap<String, List<String>> tree = new TreeMap<>(map);
		System.out.println(tree);
		int[][] expectedValues = {{1,4,3},{1,5,3},{2,3,4,5},{4,5,2,3}};
		int counter = 0;

		for(List<String> value: tree.values()){
			assert(value.size() == 1);
			String str = value.get(0).replaceAll("# ", "");
			//System.out.println(str);
			
			String[] split = str.split(" ");
			String temp1 = split[0];
			String temp2 = split[1];
			split[0] = "-2";
			split[1] = "-1";
			Arrays.sort(split);
			split[0] = temp1;
			split[1] = temp2;
			for (int i = 0; i < split.length; i++){
				//System.out.println(split[i] + " " + expectedValues[counter][i]);
				assert(Integer.parseInt(split[i]) == expectedValues[counter][i]);
			}
			counter++;
		
		}
		
	}
	

	

}

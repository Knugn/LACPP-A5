package org.uu.lacpp15.g3.mapreduce.implementations;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.uu.lacpp15.g3.mapreduce.framework.MapReduceInUtil;

public class CommonFriendsTest {

	@Test
	public void test() {
		String test = "(1,3)\n(2,3)\n(2,5)\n(3,5)";
		//String test = GraphGen.out(1000,10);r
		//Map<String,List<String>> map = CommonFriends.run(test,10);
		//System.out.print(map);
		
		
		
	}
	

	@Test
	public void testFile() {
		List<URI> inputFIle = new ArrayList<URI>();
		Path path2 = Paths.get("src/org/uu/lacpp15/g3/mapreduce/resoucre/graph1.txt");
		//System.out.println(path2.toAbsolutePath().toString());
		inputFIle.add(path2.toUri());
		
		Map<String, List<String>> map = CommonFriends.run(MapReduceInUtil.fromFileLines(inputFIle), 10);
	//	System.out.println(map.toString());
	}
	

}

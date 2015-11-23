package org.uu.lacpp15.g3.mapreduce.implementations;

import java.util.Arrays;
import java.util.List;

import org.uu.lacpp15.g3.mapreduce.framework.KeyValueEmitter;
import org.uu.lacpp15.g3.mapreduce.framework.Mapper;
import org.uu.lacpp15.g3.mapreduce.framework.Reducer;
import org.uu.lacpp15.g3.mapreduce.framework.ValueEmitter;

public class Triangles {


	public static void main(String[] args) {

	}

	public class GraphTrianglesMapper implements Mapper<String, String, Integer, int[]>{
		int n;

		GraphTrianglesMapper(int n){
			this.n = n;
		}

		@Override
		public void map(String key, String value,
				KeyValueEmitter<Integer, int[]> emitter) {
			//input is A person and B all his/her friends
			value.replaceAll("[^0-9]+", " ");
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

	public class GraphTrianglesReducer implements Reducer<Integer, int[], Integer>
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
			emitter.emit(ans);
		}

	}
}

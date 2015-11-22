package org.uu.lacpp15.g3.mapreduce.implementations;

import org.uu.lacpp15.g3.mapreduce.framework.KeyValueEmitter;
import org.uu.lacpp15.g3.mapreduce.framework.Mapper;
import org.uu.lacpp15.g3.mapreduce.framework.Reducer;

public class Triangles {


	public static void main(String[] args) {

	}

	public class GraphTrianglesMapper implements Mapper<String, String, String, Integer>{
		int n;

		GraphTrianglesMapper(int n){
			this.n = n;
		}

		@Override
		public void map(String key, String value,
				KeyValueEmitter<String, Integer> emitter) {
			//input is A person han all his/her friends
			value.replaceAll("[^0-9]+", " ");
			String[] values = value.split(" ");
			int value1 = Integer.parseInt(values[0]);
			int value2 = Integer.parseInt(values[1]);
			for (int i = 1;  i < n + 1; i++){
				if (value1 != i && value2 != i){
					String outKey = "";
					if (value1 < i){
						outKey = i + " " + value1;
					}
					else{
						outKey = value1 + " " + i;
					}
					emitter.emit(outKey, value2);

					outKey = "";
					if (value2 < i){
						outKey = i + " " + value2;
					}
					else{
						outKey = value2 + " " + i;
					}
					emitter.emit(outKey, value1);
				}
			}
		}
	}		



	public class GraphTrianglesReducer implements Reducer<String, Integer, String, String>
	{
		@Override
		public void reduce(String key, Iterable<Integer> values,
				KeyValueEmitter<String, String> emitter) {
			String ans = key + " #";
			int counter = 0;
			for (Integer value : values) {
				int internalCounter = 0;
				for (Integer value2 : values) {
					if (internalCounter > counter){
						if (value == value2){
							ans += " " + value;
						}
					}
					internalCounter++;
				}
				counter++;
			}
			emitter.emit(key, ans);
		}

	}
}

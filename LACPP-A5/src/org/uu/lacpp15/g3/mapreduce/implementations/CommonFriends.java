package org.uu.lacpp15.g3.mapreduce.implementations;

import org.uu.lacpp15.g3.mapreduce.framework.KeyValueEmitter;
import org.uu.lacpp15.g3.mapreduce.framework.Mapper;
import org.uu.lacpp15.g3.mapreduce.framework.Reducer;

public class CommonFriends {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}


	public class GraphConversionMapper implements Mapper<String, String, String, String>{

		@Override
		public void map(String key, String value,
				KeyValueEmitter<String, String> emitter) {

			String[] values = value.split(" ");
			String first = values[0];
			for (int i = 2; i < values.length; i++) {
				String newKey;
				if (first.compareTo(values[i]) > 0){
					newKey = values[i] + first;
				}else{
					newKey = first + values[i];
				}
				
				String friends = "";
				for(String otherfriend: values){
					if (otherfriend.compareTo("#") == 0){
						continue;
					}
					if(otherfriend != values[i]){
						friends += otherfriend + " ";
					}
				}
				if (friends.compareTo("") != 0){
					emitter.emit(newKey, friends);
				}
			}
		}


	}


	public class GraphConversionReducer implements Reducer<String, String, String, String>
	{
		@Override
		public void reduce(String key, Iterable<String> values,
			KeyValueEmitter<String, String> emitter) {
			
			
			
			String[] frineds = new String[2];
			int counter = 0;
			for(String value: values){
				frineds[counter] = value;
				counter++;
				
			}
			if (counter == 1){
				return;
			}
			String ans = key;
			String[] friends1 = frineds[0].split(" ");
			String[] friends2 = frineds[1].split(" ");
			//if sorted this cloud be made faster!
			for(String friend1: friends1){
				for(String friend2: friends2){
					if (friend1 == friend2){
						ans += " " + friend1;
						break;
					}
				}	
			}
			emitter.emit(key, ans);
		}
	
	}
}

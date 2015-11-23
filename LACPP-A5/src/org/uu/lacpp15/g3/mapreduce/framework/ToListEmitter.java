package org.uu.lacpp15.g3.mapreduce.framework;

import java.util.List;

public class ToListEmitter<V> implements ValueEmitter<V> {

	private List<V> list;
	
	public ToListEmitter(List<V> list) {
		super();
		setList(list);
	}
	
	public List<V> getList() {
		return list;
	}

	public void setList(List<V> list) {
		this.list = list;
	}

	@Override
	public void emit(V value) {
		list.add(value);
	}
	
}

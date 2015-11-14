package org.uu.lacpp15.g3.mapreduce.framework;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class FileInputMap<V> implements InputMap<URI, V> {
	
	TreeSet<URI>	fileUriSet;
	FileParser<V>	parser;
	
	public FileInputMap(FileParser<V> parser) {
		this(parser, null);
	}
	
	public FileInputMap(FileParser<V> parser, Collection<URI> fileUris) {
		this.fileUriSet = new TreeSet<URI>();
		if (fileUris != null)
			addFiles(fileUris);
		this.parser = parser;
	}
	
	@Override
	public Set<Entry> getEntrySet() {
		TreeSet<Entry> entries = new TreeSet<>();
		for (URI uri : fileUriSet)
			entries.add(new Entry(uri));
		return entries;
	}
	
	@Override
	public Set<URI> getKeySet() {
		return new TreeSet<URI>(fileUriSet);
	}
	
	@Override
	public List<V> getValues() {
		Set<Entry> entries = getEntrySet();
		List<V> values = new ArrayList<V>(entries.size());
		for (Entry e : entries)
			values.add(e.getValue());
		return values;
	}
	
	@Override
	public int count() {
		return fileUriSet.size();
	}
	
	public void addFile(URI file) {
		fileUriSet.add(file);
	}
	
	public void addFiles(Collection<URI> files) {
		fileUriSet.addAll(files);
	}
	
	@Override
	public List<FileInputMap<V>> split(int nParts) {
		if (fileUriSet.size() < nParts)
			nParts = fileUriSet.size();
		int quot = fileUriSet.size() / nParts;
		int rem = fileUriSet.size() % nParts;
		List<FileInputMap<V>> parts = new ArrayList<>(nParts);
		Iterator<URI> iter = fileUriSet.iterator();
		for (int i=0; i<nParts; i++) {
			int nFiles = quot;
			if (i < rem)
				nFiles++;
			List<URI> partUris = new ArrayList<>(nFiles);
			for (int j=0; j<nFiles; j++)
				partUris.add(iter.next());
			parts.add(new FileInputMap<>(parser, partUris));
		}
		return parts;
	}

	public class Entry implements InputMap.Entry<URI, V>, Comparable<Entry> {
		
		URI fileUri;
		
		public Entry(URI fileUri) {
			this.fileUri = fileUri;
		}
		
		@Override
		public URI getKey() {
			return fileUri;
		}

		@Override
		public V getValue() {
			return parser.parse(fileUri);
		}

		@Override
		public int compareTo(Entry that) {
			return this.fileUri.compareTo(that.fileUri);
		}
		
	}
	
}

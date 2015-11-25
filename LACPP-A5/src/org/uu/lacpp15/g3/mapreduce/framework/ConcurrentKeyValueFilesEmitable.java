package org.uu.lacpp15.g3.mapreduce.framework;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ConcurrentKeyValueFilesEmitable<K,V> implements ConcurrentKeyValueEmitable<K,V> {
	
	PathGenerator filePathGenerator;
	KeyValueFormatter<? super K,? super V> formatter;
	int keyValuePairsPerFile;
	Charset cs;
	
	
	
	public ConcurrentKeyValueFilesEmitable(
			PathGenerator filePathGenerator, 
			KeyValueFormatter<? super K,? super V> formatter, 
			int keyValuePairsPerFile,
			Charset cs) {
		super();
		setFilePathGenerator(filePathGenerator);
		setFormatter(formatter);
		setKeyValuePairsPerFile(keyValuePairsPerFile);
		setCharset(cs);
	}

	public Charset getCharset() {
		return cs;
	}

	public void setCharset(Charset cs) {
		if (cs == null)
			cs = Charset.defaultCharset();
		this.cs = cs;
	}

	public int getKeyValuePairsPerFile() {
		return keyValuePairsPerFile;
	}

	public void setKeyValuePairsPerFile(int keyValuePairsPerFile) {
		if (keyValuePairsPerFile < 0)
			throw new IllegalArgumentException("keyValuePairsPerFile must be > 0, or 0 for default value.");
		if (keyValuePairsPerFile == 0)
			keyValuePairsPerFile = Integer.MAX_VALUE;
		this.keyValuePairsPerFile = keyValuePairsPerFile;
	}

	public PathGenerator getFilePathGenerator() {
		return filePathGenerator;
	}

	public void setFilePathGenerator(PathGenerator filePathGenerator) {
		if (filePathGenerator == null)
			throw new IllegalArgumentException("filePathGenerator must not be null.");
		this.filePathGenerator = filePathGenerator;
	}

	public KeyValueFormatter<? super K, ? super V> getFormatter() {
		return formatter;
	}

	public void setFormatter(KeyValueFormatter<? super K, ? super V> formatter) {
		if(formatter == null)
			throw new IllegalArgumentException("formatter must not be null.");
		this.formatter = formatter;
	}

	@Override
	public Collection<KeyValueEmitter<K,V>> emitters(int numEmitters) {
		EmitterSession session = new EmitterSession();
		List<KeyValueEmitter<K,V>> emitters = new ArrayList<>(numEmitters);
		for (int i=0; i<numEmitters; i++)
			emitters.add(session.new Emitter());
		return emitters;
	}
	
	private class EmitterSession {
		
		BufferedWriter curWriter;
		int lineCount;
		
		private class Emitter implements KeyValueEmitter<K,V>{

			@Override
			public void emit(K key, V value) {
				String line = formatter.format(key, value);
				try {
					synchronized(EmitterSession.this) {
						if (curWriter == null) {
							curWriter = Files.newBufferedWriter(filePathGenerator.next(), cs);
							lineCount = 0;
						}
						curWriter.write(line);
						curWriter.newLine();
						curWriter.flush();
						lineCount++;
						if (lineCount >= keyValuePairsPerFile) {
							curWriter.close();
							curWriter = null;
						}
					}
				}
				catch (IOException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
		}
		
	}
}

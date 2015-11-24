package org.uu.lacpp15.g3.mapreduce.framework;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ConcurrentKeyValueFilesEmitable<K,V> implements ConcurrentKeyValueEmitable<K,V> {
	
	Charset cs;
	int keyValuePairsPerFile;
	PathGenerator filePathGenerator;
	String keyPairFormat;
	
	public ConcurrentKeyValueFilesEmitable(
			PathGenerator filePathGenerator, 
			String keyPairFormat, 
			int keyValuePairsPerFile,
			Charset cs) {
		super();
		setCharset(cs);
		setKeyValuePairsPerFile(keyValuePairsPerFile);
		setFilePathGenerator(filePathGenerator);
		setKeyPairFormat(keyPairFormat);
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

	public String getKeyPairFormat() {
		return keyPairFormat;
	}

	public void setKeyPairFormat(String keyPairFormat) {
		if(keyPairFormat == null)
			throw new IllegalArgumentException("keyPairFormat must not be null.");
		this.keyPairFormat = keyPairFormat;
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
				String line = String.format(keyPairFormat, key, value);
				try {
					synchronized(EmitterSession.this) {
						if (curWriter == null) {
							curWriter = Files.newBufferedWriter(filePathGenerator.next(), cs);
							lineCount = 0;
						}
						curWriter.write(line);
						curWriter.newLine();
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

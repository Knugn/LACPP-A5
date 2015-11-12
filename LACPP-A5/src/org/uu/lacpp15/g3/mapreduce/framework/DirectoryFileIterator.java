package org.uu.lacpp15.g3.mapreduce.framework;

import java.io.File;
import java.io.IOException;

public abstract class DirectoryFileIterator<V> implements KeyValueIterator<String, V>{
	
	File dir;
	File[] dirFiles;
	int fileIdx = -1;
	
	public DirectoryFileIterator(String path) throws IOException {
		dir = new File(path);
		if (!dir.isDirectory())
			throw new IllegalArgumentException("Given path is not a directory.");
		File[] directoryListing = dir.listFiles();
		if (directoryListing == null) {
			// Checking dir.isDirectory() above would not be sufficient
			// to avoid race conditions with another process that deletes
			// directories.
			throw new IOException("Failed to list files in directory. Was it deleted?");
		}
	}
	
	@Override
	public boolean next() {
		return ++fileIdx < dirFiles.length;
	}

	@Override
	public String getKey() {
		return dirFiles[fileIdx].getName();
	}
	
	protected File getCurrentFile() {
		return dirFiles[fileIdx];
	}
	
}

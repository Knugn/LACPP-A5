package org.uu.lacpp15.g3.mapreduce.framework;

import java.io.IOException;
import java.nio.file.Files;

public class DirectoryByteFileIterator extends DirectoryFileIterator<byte[]> {

	public DirectoryByteFileIterator(String path) throws IOException {
		super(path);
	}

	@Override
	public byte[] getValue() {
		try {
			return Files.readAllBytes(getCurrentFile().toPath());
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
}

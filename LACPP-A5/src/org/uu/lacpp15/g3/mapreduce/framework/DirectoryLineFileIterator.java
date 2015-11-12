package org.uu.lacpp15.g3.mapreduce.framework;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;

public class DirectoryLineFileIterator extends DirectoryFileIterator<List<String>> {
	
	Charset charset;
	
	public DirectoryLineFileIterator(String path) throws IOException {
		this(path, Charset.defaultCharset());
	}
	
	public DirectoryLineFileIterator(String path, Charset charset) throws IOException {
		super(path);
		setCharset(charset);
	}

	public Charset getCharset() {
		return charset;
	}

	public void setCharset(Charset charset) {
		this.charset = charset;
	}
	
	@Override
	public List<String> getValue() {
		try {
			return Files.readAllLines(getCurrentFile().toPath(), charset);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}

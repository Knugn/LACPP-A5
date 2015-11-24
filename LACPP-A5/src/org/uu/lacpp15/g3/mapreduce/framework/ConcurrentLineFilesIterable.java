package org.uu.lacpp15.g3.mapreduce.framework;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ConcurrentLineFilesIterable implements ConcurrentKeyValueIterable<String,String> {
	
	Iterable<URI> fileUris;

	public ConcurrentLineFilesIterable(Iterable<URI> fileUris) {
		super();
		setFileUris(fileUris);
	}

	public Iterable<URI> getFileUris() {
		return fileUris;
	}

	public void setFileUris(Iterable<URI> fileUris) {
		this.fileUris = fileUris;
	}

	@Override
	public Collection<KeyValueIterator<String, String>> iterators(int numIterators) {
		ConcurrentIteratorsSession session = new ConcurrentIteratorsSession(fileUris.iterator());
		List<KeyValueIterator<String,String>> iters = new ArrayList<>(numIterators);
		for (int i=0; i<numIterators; i++)
			iters.add(session.new ChunkIterator());
		return iters;
	}
	
	private class ConcurrentIteratorsSession {
		
		private Charset				cs					= Charset.defaultCharset();
		private int					linesPerChunk		= 16;
		private Iterator<URI>		fileIterator 		= null;
		private List<String>		curFileContent		= null;
		private Iterator<String>	curFileContentIter	= null;
		private int					curFileCursor		= -1;
		private boolean				done				= false;
		
		public ConcurrentIteratorsSession(Iterator<URI> fileIter) {
			this.fileIterator = fileIter;
		}
		
		private class ChunkIterator implements KeyValueIterator<String, String> {
			String				keyFormat			= null;
			List<String>		chunkLines			= new ArrayList<>(linesPerChunk);
			Iterator<String>	chunkLinesIter		= null;
			int					nextChunkLineNumber	= -1;
			String				curKey				= null;
			String				curLine				= null;
			
			private boolean getChunk() {
				synchronized (ConcurrentIteratorsSession.this) {
					while (curFileCursor < 0) {
						if (!fileIterator.hasNext()) {
							done = true;
							return false;
						}
						Path path = Paths.get(fileIterator.next());
						try {
							curFileContent = Files.readAllLines(path, cs);
						}
						catch (IOException e) {
							e.printStackTrace();
							throw new RuntimeException(e);
						}
						if (curFileContent.size() <= 0) {
							curFileCursor = -1;
							curFileContentIter = null;
						}
						else {
							curFileCursor = 0;
							curFileContentIter = curFileContent.iterator();
							keyFormat = "<File="+path.toString() +",Line=%d>";
						}
					}
					chunkLines.clear();
					nextChunkLineNumber = curFileCursor;
					for (int i=0; i<linesPerChunk; i++) {
						if (curFileContentIter.hasNext())
							chunkLines.add(curFileContentIter.next());
						else {
							curFileCursor = -1;
							return true;
						}
					}
					if (!curFileContentIter.hasNext())
						curFileCursor = -1;
				}
				return true;
			}
			
			@Override
			public boolean next() {
				if (done)
					return false;
				if (chunkLinesIter == null || !chunkLinesIter.hasNext()) {
					if (!getChunk())
						return false;
					chunkLinesIter = chunkLines.iterator();
				}
				curKey = String.format(keyFormat, nextChunkLineNumber);
				curLine = chunkLinesIter.next();
				nextChunkLineNumber++;
				return true;
			}
			
			@Override
			public String getKey() {
				return curKey;
			}
			
			@Override
			public String getValue() {
				return curLine;
			}
		}
	}
	
}

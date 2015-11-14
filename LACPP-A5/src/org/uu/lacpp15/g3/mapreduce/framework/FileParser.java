package org.uu.lacpp15.g3.mapreduce.framework;

import java.io.File;
import java.net.URI;

public interface FileParser<V> {
	V parse(File file);
	V parse(URI fileUri);
}

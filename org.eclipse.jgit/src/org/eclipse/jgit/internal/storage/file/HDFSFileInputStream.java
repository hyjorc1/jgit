package org.eclipse.jgit.internal.storage.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * @author hyj
 *
 */
public class HDFSFileInputStream extends ByteArrayInputStream {

	/**
	 * @param file
	 */
	public HDFSFileInputStream(HDFSFile file) {
		super(file.getBytes());
	}

	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {
			// IGNOREME
		}
	}
}

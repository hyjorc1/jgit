package org.eclipse.jgit.internal.storage.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * @author hyj
 *
 */
public class ByteArrayFileInputStream extends ByteArrayInputStream {

	/**
	 * @param file
	 */
	public ByteArrayFileInputStream(ByteArrayFile file) {
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

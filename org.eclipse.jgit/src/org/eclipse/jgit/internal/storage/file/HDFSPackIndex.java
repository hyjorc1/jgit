package org.eclipse.jgit.internal.storage.file;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;

import org.eclipse.jgit.errors.CorruptObjectException;
import org.eclipse.jgit.errors.UnsupportedPackIndexVersionException;
import org.eclipse.jgit.internal.JGitText;
import org.eclipse.jgit.util.IO;
import org.eclipse.jgit.util.NB;
import org.eclipse.jgit.util.io.SilentFileInputStream;

/**
 * @author hyj
 *
 */
public abstract class HDFSPackIndex extends PackIndex {

	// TODO use HDFSFIleInputStream
	public static PackIndex open(File idxFile) throws IOException {
		try (SilentFileInputStream fd = new SilentFileInputStream(idxFile)) {
			return read(fd);
		} catch (IOException ioe) {
			throw new IOException(
					MessageFormat.format(JGitText.get().unreadablePackIndex,
							idxFile.getAbsolutePath()),
					ioe);
		}
	}

	public static PackIndex read(InputStream fd)
			throws IOException, CorruptObjectException {
		final byte[] hdr = new byte[8];
		IO.readFully(fd, hdr, 0, hdr.length);
		if (isTOC(hdr)) {
			final int v = NB.decodeInt32(hdr, 4);
			switch (v) {
			case 2:
				return new PackIndexV2(fd);
			default:
				throw new UnsupportedPackIndexVersionException(v);
			}
		}
		return new PackIndexV1(fd, hdr);
	}

	private static boolean isTOC(byte[] h) {
		final byte[] toc = PackIndexWriter.TOC;
		for (int i = 0; i < toc.length; i++)
			if (h[i] != toc[i])
				return false;
		return true;
	}
}

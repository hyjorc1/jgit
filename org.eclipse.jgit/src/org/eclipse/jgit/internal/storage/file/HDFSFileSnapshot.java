package org.eclipse.jgit.internal.storage.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.eclipse.jgit.util.FS;

/**
 * @author hyj
 *
 */
public class HDFSFileSnapshot {
	/**
	 * An unknown file size.
	 *
	 * This value is used when a comparison needs to happen purely on the lastUpdate.
	 */
	public static final long UNKNOWN_SIZE = -1;

	/**
	 * A HDFSFileSnapshot that is considered to always be modified.
	 * <p>
	 * This instance is useful for application code that wants to lazily read a
	 * file, but only after {@link #isModified(File)} gets invoked. The returned
	 * snapshot contains only invalid status information.
	 */
	public static final HDFSFileSnapshot DIRTY = new HDFSFileSnapshot(-1, -1, UNKNOWN_SIZE);

	/**
	 * A HDFSFileSnapshot that is clean if the file does not exist.
	 * <p>
	 * This instance is useful if the application wants to consider a missing
	 * file to be clean. {@link #isModified(File)} will return false if the file
	 * path does not exist.
	 */
	public static final HDFSFileSnapshot MISSING_FILE = new HDFSFileSnapshot(0, 0, 0) {
		@Override
		public boolean isModified(File path) {
			return FS.DETECTED.exists(path);
		}
	};

	/**
	 * Record a snapshot for a specific file path.
	 * <p>
	 * This method should be invoked before the file is accessed.
	 *
	 * @param path
	 *            the path to later remember. The path's current status
	 *            information is saved.
	 * @return the snapshot.
	 */
	// different of FileSnaspshot
	public static HDFSFileSnapshot save(File path) {
		long read = System.currentTimeMillis();
		long modified;
		long size;
		try {
			BasicFileAttributes fileAttributes = FS.DETECTED.fileAttributes(path);
			modified = fileAttributes.lastModifiedTime().toMillis();
			size = fileAttributes.size();
		} catch (IOException e) {
			modified = path.lastModified();
			size = path.length();
		}
		return new HDFSFileSnapshot(read, modified, size);
	}

	/**
	 * Record a snapshot for a file for which the last modification time is
	 * already known.
	 * <p>
	 * This method should be invoked before the file is accessed.
	 *
	 * @param modified
	 *            the last modification time of the file
	 * @return the snapshot.
	 */
	public static HDFSFileSnapshot save(long modified) {
		final long read = System.currentTimeMillis();
		return new HDFSFileSnapshot(read, modified, -1);
	}

	/** Last observed modification time of the path. */
	private final long lastModified;

	/** Last wall-clock time the path was read. */
	private volatile long lastRead;

	/** True once {@link #lastRead} is far later than {@link #lastModified}. */
	private boolean cannotBeRacilyClean;

	/** Underlying file-system size in bytes.
	 *
	 * When set to {@link #UNKNOWN_SIZE} the size is not considered for modification checks. */
	private final long size;

	private HDFSFileSnapshot(long read, long modified, long size) {
		this.lastRead = read;
		this.lastModified = modified;
		this.cannotBeRacilyClean = notRacyClean(read);
		this.size = size;
	}

	/**
	 * Get time of last snapshot update
	 *
	 * @return time of last snapshot update
	 */
	public long lastModified() {
		return lastModified;
	}

	/**
	 * @return file size in bytes of last snapshot update
	 */
	public long size() {
		return size;
	}

	/**
	 * Check if the path may have been modified since the snapshot was saved.
	 *
	 * @param path
	 *            the path the snapshot describes.
	 * @return true if the path needs to be read again.
	 */
	public boolean isModified(File path) {
		long currLastModified;
		long currSize;
		try {
			BasicFileAttributes fileAttributes = FS.DETECTED.fileAttributes(path);
			currLastModified = fileAttributes.lastModifiedTime().toMillis();
			currSize = fileAttributes.size();
		} catch (IOException e) {
			currLastModified = path.lastModified();
			currSize = path.length();
		}
		return (currSize != UNKNOWN_SIZE && currSize != size) || isModified(currLastModified);
	}

	/**
	 * Update this snapshot when the content hasn't changed.
	 * <p>
	 * If the caller gets true from {@link #isModified(File)}, re-reads the
	 * content, discovers the content is identical, and
	 * {@link #equals(HDFSFileSnapshot)} is true, it can use
	 * {@link #setClean(HDFSFileSnapshot)} to make a future
	 * {@link #isModified(File)} return false. The logic goes something like
	 * this:
	 *
	 * <pre>
	 * if (snapshot.isModified(path)) {
	 *  HDFSFileSnapshot other = HDFSFileSnapshot.save(path);
	 *  Content newContent = ...;
	 *  if (oldContent.equals(newContent) &amp;&amp; snapshot.equals(other))
	 *      snapshot.setClean(other);
	 * }
	 * </pre>
	 *
	 * @param other
	 *            the other snapshot.
	 */
	public void setClean(HDFSFileSnapshot other) {
		final long now = other.lastRead;
		if (notRacyClean(now))
			cannotBeRacilyClean = true;
		lastRead = now;
	}

	/**
	 * Compare two snapshots to see if they cache the same information.
	 *
	 * @param other
	 *            the other snapshot.
	 * @return true if the two snapshots share the same information.
	 */
	public boolean equals(HDFSFileSnapshot other) {
		return lastModified == other.lastModified;
	}

	/** {@inheritDoc} */
	@Override
	public boolean equals(Object other) {
		if (other instanceof HDFSFileSnapshot)
			return equals((HDFSFileSnapshot) other);
		return false;
	}

	/** {@inheritDoc} */
	@Override
	public int hashCode() {
		// This is pretty pointless, but override hashCode to ensure that
		// x.hashCode() == y.hashCode() when x.equals(y) is true.
		//
		return (int) lastModified;
	}

	/** {@inheritDoc} */
	@Override
	public String toString() {
		if (this == DIRTY)
			return "DIRTY"; //$NON-NLS-1$
		if (this == MISSING_FILE)
			return "MISSING_FILE"; //$NON-NLS-1$
		DateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", //$NON-NLS-1$
				Locale.US);
		return "HDFSFileSnapshot[modified: " + f.format(new Date(lastModified)) //$NON-NLS-1$
				+ ", read: " + f.format(new Date(lastRead)) + "]"; //$NON-NLS-1$ //$NON-NLS-2$
	}

	private boolean notRacyClean(long read) {
		// The last modified time granularity of FAT filesystems is 2 seconds.
		// Using 2.5 seconds here provides a reasonably high assurance that
		// a modification was not missed.
		//
		return read - lastModified > 2500;
	}

	private boolean isModified(long currLastModified) {
		// Any difference indicates the path was modified.
		//
		if (lastModified != currLastModified)
			return true;

		// We have already determined the last read was far enough
		// after the last modification that any new modifications
		// are certain to change the last modified time.
		//
		if (cannotBeRacilyClean)
			return false;

		if (notRacyClean(lastRead)) {
			// Our last read should have marked cannotBeRacilyClean,
			// but this thread may not have seen the change. The read
			// of the volatile field lastRead should have fixed that.
			//
			return false;
		}

		// We last read this path too close to its last observed
		// modification time. We may have missed a modification.
		// Scan again, to ensure we still see the same state.
		//
		return true;
	}
}

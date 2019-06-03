package org.eclipse.jgit.internal.storage.file;

import static org.eclipse.jgit.internal.storage.pack.PackExt.BITMAP_INDEX;
import static org.eclipse.jgit.internal.storage.pack.PackExt.INDEX;
import static org.eclipse.jgit.internal.storage.pack.PackExt.KEEP;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.DataFormatException;

import org.eclipse.jgit.errors.CorruptObjectException;
import org.eclipse.jgit.errors.LargeObjectException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.errors.PackInvalidException;
import org.eclipse.jgit.errors.PackMismatchException;
import org.eclipse.jgit.internal.JGitText;
import org.eclipse.jgit.internal.storage.pack.BinaryDelta;
import org.eclipse.jgit.internal.storage.pack.PackExt;
import org.eclipse.jgit.internal.storage.pack.PackOutputStream;
import org.eclipse.jgit.lib.AbbreviatedObjectId;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.util.LongList;

/**
 * @author hyj
 *
 */
public class ByteArrayPackFile extends PackFile {
	/**
	 * Sorts PackFiles to be most recently created to least recently created.
	 */

	private final ByteArrayFile packFile;

	private final int extensions;

	private ByteArrayFile keepFile;

	private volatile String packName;

	final int HDFS_hash;

	private ByteArrayFileInputStream fd;

	/** Serializes reads performed against. */
	private final Object readLock = new Object();

	private int activeWindows;

	private int activeCopyRawData;

	private FileSnapshot fileSnapshot;

	private volatile boolean invalid;

	private volatile Exception invalidatingCause;

	private boolean invalidBitmap;

	private AtomicInteger transientErrorCount = new AtomicInteger();

	private byte[] packChecksum;

	private volatile PackIndex loadedIdx;

	private PackReverseIndex reverseIdx;

	private PackBitmapIndex bitmapIdx;

	/**
	 * Objects we have tried to read, and discovered to be corrupt.
	 * <p>
	 * The list is allocated after the first corruption is found, and filled in
	 * as more entries are discovered. Typically this list is never used, as
	 * pack files do not usually contain corrupt objects.
	 */
	private volatile LongList corruptObjects;

	/**
	 * Construct a reader for an existing, pre-indexed packfile.
	 *
	 * @param packFile
	 *            path of the <code>.pack</code> file holding the data.
	 * @param extensions
	 *            additional pack file extensions with the same base as the pack
	 */
	public ByteArrayPackFile(ByteArrayFile packFile, int extensions) {
		super(packFile, extensions);
		this.packFile = packFile;
		this.fileSnapshot = FileSnapshot.save(packFile);
		this.packLastModified = (int) (fileSnapshot.lastModified() >> 10);
		this.extensions = extensions;

		// Multiply by 31 here so we can more directly combine with another
		// value in WindowCache.hash(), without doing the multiply there.
		//
		HDFS_hash = System.identityHashCode(this) * 31;
		length = Long.MAX_VALUE;
	}

	private PackIndex idx() throws IOException {
		PackIndex idx = loadedIdx;
		if (idx == null) {
			synchronized (this) {
				idx = loadedIdx;
				if (idx == null) {
					if (invalid) {
						throw new PackInvalidException(packFile,
								invalidatingCause);
					}
					try {
						// CHECKME use HDFSPackIndex
						idx = ByteArrayPackIndex.open(extFile(INDEX));

						if (packChecksum == null) {
							packChecksum = idx.packChecksum;
						} else if (!Arrays.equals(packChecksum,
								idx.packChecksum)) {
							throw new PackMismatchException(MessageFormat
									.format(JGitText.get().packChecksumMismatch,
											packFile.getPath(),
											ObjectId.fromRaw(packChecksum)
													.name(),
											ObjectId.fromRaw(idx.packChecksum)
													.name()));
						}
						loadedIdx = idx;
					} catch (InterruptedIOException e) {
						// don't invalidate the pack, we are interrupted from
						// another thread
						throw e;
					} catch (IOException e) {
						invalid = true;
						invalidatingCause = e;
						throw e;
					}
				}
			}
		}
		return idx;
	}

	/**
	 * Get the HDFSFile object which locates this pack on disk.
	 *
	 * @return the HDFSFile object which locates this pack on disk.
	 */
	@Override
	public ByteArrayFile getPackFile() {
		return packFile;
	}

	/**
	 * Get the index for this pack file.
	 *
	 * @return the index for this pack file.
	 * @throws java.io.IOException
	 */
	@Override
	public PackIndex getIndex() throws IOException {
		return idx();
	}

	/**
	 * Get name extracted from {@code pack-*.pack} pattern.
	 *
	 * @return name extracted from {@code pack-*.pack} pattern.
	 */
	@Override
	public String getPackName() {
		String name = packName;
		if (name == null) {
			name = getPackFile().getName();
			if (name.startsWith("pack-")) //$NON-NLS-1$
				name = name.substring("pack-".length()); //$NON-NLS-1$
			if (name.endsWith(".pack")) //$NON-NLS-1$
				name = name.substring(0, name.length() - ".pack".length()); //$NON-NLS-1$
			packName = name;
		}
		return name;
	}

	/**
	 * Determine if an object is contained within the pack file.
	 * <p>
	 * For performance reasons only the index file is searched; the main pack
	 * content is ignored entirely.
	 * </p>
	 *
	 * @param id
	 *            the object to look for. Must not be null.
	 * @return true if the object is in this pack; false otherwise.
	 * @throws java.io.IOException
	 *             the index file cannot be loaded into memory.
	 */
	@Override
	public boolean hasObject(AnyObjectId id) throws IOException {
		final long offset = idx().findOffset(id);
		return 0 < offset && !isCorrupt(offset);
	}

	/**
	 * Determines whether a .keep file exists for this pack file.
	 *
	 * @return true if a .keep file exist.
	 */
	@Override
	public boolean shouldBeKept() {
		if (keepFile == null)
			keepFile = extFile(KEEP);
		return keepFile.exists();
	}

	/**
	 * Get an object from this pack.
	 *
	 * @param curs
	 *            temporary working space associated with the calling thread.
	 * @param id
	 *            the object to obtain from the pack. Must not be null.
	 * @return the object loader for the requested object if it is contained in
	 *         this pack; null if the object was not found.
	 * @throws IOException
	 *             the pack file or the index could not be read.
	 */
	@Override
	ObjectLoader get(WindowCursor curs, AnyObjectId id) throws IOException {
		final long offset = idx().findOffset(id); // CHECKME
		return 0 < offset && !isCorrupt(offset) ? load(curs, offset) : null;
	}

	@Override
	void resolve(Set<ObjectId> matches, AbbreviatedObjectId id, int matchLimit)
			throws IOException {
		idx().resolve(matches, id, matchLimit);
	}

	/**
	 * Close the resources utilized by this repository
	 */
	@Override
	public void close() {
		WindowCache.purge(this);
		synchronized (this) {
			loadedIdx = null;
			reverseIdx = null;
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Provide iterator over entries in associated pack index, that should also
	 * exist in this pack file. Objects returned by such iterator are mutable
	 * during iteration.
	 * <p>
	 * Iterator returns objects in SHA-1 lexicographical order.
	 * </p>
	 *
	 * @see PackIndex#iterator()
	 */
	@Override
	public Iterator<PackIndex.MutableEntry> iterator() {
		try {
			return idx().iterator();
		} catch (IOException e) {
			return Collections.<PackIndex.MutableEntry> emptyList().iterator();
		}
	}

	/**
	 * Obtain the total number of objects available in this pack. This method
	 * relies on pack index, giving number of effectively available objects.
	 *
	 * @return number of objects in index of this pack, likewise in this pack
	 * @throws IOException
	 *             the index file cannot be loaded into memory.
	 */
	@Override
	long getObjectCount() throws IOException {
		return idx().getObjectCount();
	}

	/**
	 * Search for object id with the specified start offset in associated pack
	 * (reverse) index.
	 *
	 * @param offset
	 *            start offset of object to find
	 * @return object id for this offset, or null if no object was found
	 * @throws IOException
	 *             the index file cannot be loaded into memory.
	 */
	@Override
	ObjectId findObjectForOffset(long offset) throws IOException {
		return getReverseIdx().findObject(offset);
	}

	/**
	 * Return the @{@link FileSnapshot} associated to the underlying packfile
	 * that has been used when the object was created.
	 *
	 * @return the packfile @{@link FileSnapshot} that the object is loaded
	 *         from.
	 */
	@Override
	FileSnapshot getFileSnapshot() {
		return fileSnapshot;
	}

	private final byte[] decompress(final long position, final int sz,
			final WindowCursor curs) throws IOException, DataFormatException {
		byte[] dstbuf;
		try {
			dstbuf = new byte[sz];
		} catch (OutOfMemoryError noMemory) {
			// The size may be larger than our heap allows, return null to
			// let the caller know allocation isn't possible and it should
			// use the large object streaming approach instead.
			//
			// For example, this can occur when sz is 640 MB, and JRE
			// maximum heap size is only 256 MB. Even if the JRE has
			// 200 MB free, it cannot allocate a 640 MB byte array.
			return null;
		}

		if (curs.inflate(this, position, dstbuf, false) != sz)
			throw new EOFException(
					MessageFormat.format(JGitText.get().shortCompressedStreamAt,
							Long.valueOf(position)));
		return dstbuf;
	}

	@Override
	void copyPackAsIs(PackOutputStream out, WindowCursor curs)
			throws IOException {
		// Pin the first window, this ensures the length is accurate.
		curs.pin(this, 0);
		curs.copyPackAsIs(this, length, out);
	}

	@Override
	boolean invalid() {
		return invalid;
	}

	@Override
	void setInvalid() {
		invalid = true;
	}

	@Override
	int incrementTransientErrorCount() {
		return transientErrorCount.incrementAndGet();
	}

	@Override
	void resetTransientErrorCount() {
		transientErrorCount.set(0);
	}

	private void readFully(final long position, final byte[] dstbuf, int dstoff,
			final int cnt, final WindowCursor curs) throws IOException {
		if (curs.copy(this, position, dstbuf, dstoff, cnt) != cnt) // CHECKME
			throw new EOFException();
	}

	@Override
	synchronized boolean beginWindowCache() throws IOException {
		if (++activeWindows == 1) {
			if (activeCopyRawData == 0)
				doOpen(); // CHECKME
			return true;
		}
		return false;
	}

	@Override
	synchronized boolean endWindowCache() {
		final boolean r = --activeWindows == 0;
		if (r && activeCopyRawData == 0)
			doClose();
		return r;
	}

	private void doOpen() throws IOException {
		if (invalid) {
			throw new PackInvalidException(packFile, invalidatingCause);
		}
		try {
			synchronized (readLock) {
				// CHECKME
				fd = new ByteArrayFileInputStream(packFile);
				length = packFile.length();
			}
		} catch (RuntimeException ge) {
			// generic exceptions could be transient so we should not mark the
			// pack invalid to avoid false MissingObjectExceptions
			openFail(false, ge);
			throw ge;
		}
	}

	private void openFail(boolean invalidate, Exception cause) {
		activeWindows = 0;
		activeCopyRawData = 0;
		invalid = invalidate;
		invalidatingCause = cause;
		doClose();
	}

	private void doClose() {
		synchronized (readLock) {
			if (fd != null) {
				fd.close();
				fd = null;
			}
		}
	}

	@Override
	ByteArrayWindow read(long pos, int size) throws IOException {
		synchronized (readLock) {
			if (invalid || fd == null) {
				// Due to concurrency between a read and another packfile
				// invalidation thread
				// one thread could come up to this point and then fail with
				// NPE.
				// Detect the situation and throw a proper exception so that can
				// be properly
				// managed by the main packfile search loop and the Git client
				// won't receive
				// any failures.
				throw new PackInvalidException(packFile, invalidatingCause);
			}
			if (length < pos + size)
				size = (int) (length - pos);
			final byte[] buf = new byte[size];
			fd.reset();
			fd.skip(pos);
			fd.read(buf, 0, size);
			return new ByteArrayWindow(this, pos, buf);
		}
	}

	@Override
	ByteWindow mmap(long pos, int size) throws IOException {
		return null;
	}

	@Override
	ObjectLoader load(WindowCursor curs, long pos)
			throws IOException, LargeObjectException {
		try {
			final byte[] ib = curs.tempId;
			Delta delta = null;
			byte[] data = null;
			int type = Constants.OBJ_BAD;
			boolean cached = false;

			SEARCH: for (;;) {
				// CHECKME
				readFully(pos, ib, 0, 20, curs);
				int c = ib[0] & 0xff;
				final int typeCode = (c >> 4) & 7;
				long sz = c & 15;
				int shift = 4;
				int p = 1;
				while ((c & 0x80) != 0) {
					c = ib[p++] & 0xff;
					sz += ((long) (c & 0x7f)) << shift;
					shift += 7;
				}

				switch (typeCode) {
				case Constants.OBJ_COMMIT:
				case Constants.OBJ_TREE:
				case Constants.OBJ_BLOB:
				case Constants.OBJ_TAG: {
					if (delta != null || sz < curs.getStreamFileThreshold())
						data = decompress(pos + p, (int) sz, curs);

					if (delta != null) {
						type = typeCode;
						break SEARCH;
					}

					if (data != null)
						return new ObjectLoader.SmallObject(typeCode, data);
					else
						return new LargePackedWholeObject(typeCode, sz, pos, p,
								this, curs.db);
				}

				case Constants.OBJ_OFS_DELTA: {
					c = ib[p++] & 0xff;
					long base = c & 127;
					while ((c & 128) != 0) {
						base += 1;
						c = ib[p++] & 0xff;
						base <<= 7;
						base += (c & 127);
					}
					base = pos - base;
					delta = new Delta(delta, pos, (int) sz, p, base);
					if (sz != delta.deltaSize)
						break SEARCH;

					DeltaBaseCache.Entry e = curs.getDeltaBaseCache().get(this,
							base);
					if (e != null) {
						type = e.type;
						data = e.data;
						cached = true;
						break SEARCH;
					}
					pos = base;
					continue SEARCH;
				}

				case Constants.OBJ_REF_DELTA: {
					readFully(pos + p, ib, 0, 20, curs);
					long base = findDeltaBase(ObjectId.fromRaw(ib));
					delta = new Delta(delta, pos, (int) sz, p + 20, base);
					if (sz != delta.deltaSize)
						break SEARCH;

					DeltaBaseCache.Entry e = curs.getDeltaBaseCache().get(this,
							base);
					if (e != null) {
						type = e.type;
						data = e.data;
						cached = true;
						break SEARCH;
					}
					pos = base;
					continue SEARCH;
				}

				default:
					throw new IOException(MessageFormat.format(
							JGitText.get().unknownObjectType,
							Integer.valueOf(typeCode)));
				}
			}

			// At this point there is at least one delta to apply to data.
			// (Whole objects with no deltas to apply return early above.)

			if (data == null)
				throw new IOException(
						JGitText.get().inMemoryBufferLimitExceeded);

			assert (delta != null);
			do {
				// Cache only the base immediately before desired object.
				if (cached)
					cached = false;
				else if (delta.next == null)
					curs.getDeltaBaseCache().store(this, delta.basePos, data,
							type);

				pos = delta.deltaPos;

				final byte[] cmds = decompress(pos + delta.hdrLen,
						delta.deltaSize, curs);
				if (cmds == null) {
					data = null; // Discard base in case of OutOfMemoryError
					throw new LargeObjectException.OutOfMemory(
							new OutOfMemoryError());
				}

				final long sz = BinaryDelta.getResultSize(cmds);
				if (Integer.MAX_VALUE <= sz)
					throw new LargeObjectException.ExceedsByteArrayLimit();

				final byte[] result;
				try {
					result = new byte[(int) sz];
				} catch (OutOfMemoryError tooBig) {
					data = null; // Discard base in case of OutOfMemoryError
					throw new LargeObjectException.OutOfMemory(tooBig);
				}

				BinaryDelta.apply(data, cmds, result);
				data = result;
				delta = delta.next;
			} while (delta != null);

			return new ObjectLoader.SmallObject(type, data);

		} catch (DataFormatException dfe) {
			throw new CorruptObjectException(MessageFormat.format(
					JGitText.get().objectAtHasBadZlibStream, Long.valueOf(pos),
					getPackFile()), dfe);
		}
	}

	private long findDeltaBase(ObjectId baseId)
			throws IOException, MissingObjectException {
		long ofs = idx().findOffset(baseId);
		if (ofs < 0)
			throw new MissingObjectException(baseId,
					JGitText.get().missingDeltaBase);
		return ofs;
	}

	private static class Delta {
		/** Child that applies onto this object. */
		final Delta next;

		/** Offset of the delta object. */
		final long deltaPos;

		/** Size of the inflated delta stream. */
		final int deltaSize;

		/** Total size of the delta's pack entry header (including base). */
		final int hdrLen;

		/** Offset of the base object this delta applies onto. */
		final long basePos;

		Delta(Delta next, long ofs, int sz, int hdrLen, long baseOffset) {
			this.next = next;
			this.deltaPos = ofs;
			this.deltaSize = sz;
			this.hdrLen = hdrLen;
			this.basePos = baseOffset;
		}
	}

	@Override
	byte[] getDeltaHeader(WindowCursor wc, long pos)
			throws IOException, DataFormatException {
		// The delta stream starts as two variable length integers. If we
		// assume they are 64 bits each, we need 16 bytes to encode them,
		// plus 2 extra bytes for the variable length overhead. So 18 is
		// the longest delta instruction header.
		//
		final byte[] hdr = new byte[18];
		wc.inflate(this, pos, hdr, true /* headerOnly */);
		return hdr;
	}

	@Override
	int getObjectType(WindowCursor curs, long pos) throws IOException {
		final byte[] ib = curs.tempId;
		for (;;) {
			readFully(pos, ib, 0, 20, curs);
			int c = ib[0] & 0xff;
			final int type = (c >> 4) & 7;

			switch (type) {
			case Constants.OBJ_COMMIT:
			case Constants.OBJ_TREE:
			case Constants.OBJ_BLOB:
			case Constants.OBJ_TAG:
				return type;

			case Constants.OBJ_OFS_DELTA: {
				int p = 1;
				while ((c & 0x80) != 0)
					c = ib[p++] & 0xff;
				c = ib[p++] & 0xff;
				long ofs = c & 127;
				while ((c & 128) != 0) {
					ofs += 1;
					c = ib[p++] & 0xff;
					ofs <<= 7;
					ofs += (c & 127);
				}
				pos = pos - ofs;
				continue;
			}

			case Constants.OBJ_REF_DELTA: {
				int p = 1;
				while ((c & 0x80) != 0)
					c = ib[p++] & 0xff;
				readFully(pos + p, ib, 0, 20, curs);
				pos = findDeltaBase(ObjectId.fromRaw(ib));
				continue;
			}

			default:
				throw new IOException(
						MessageFormat.format(JGitText.get().unknownObjectType,
								Integer.valueOf(type)));
			}
		}
	}

	@Override
	long getObjectSize(WindowCursor curs, AnyObjectId id) throws IOException {
		final long offset = idx().findOffset(id);
		return 0 < offset ? getObjectSize(curs, offset) : -1;
	}

	@Override
	long getObjectSize(WindowCursor curs, long pos) throws IOException {
		final byte[] ib = curs.tempId;
		readFully(pos, ib, 0, 20, curs);
		int c = ib[0] & 0xff;
		final int type = (c >> 4) & 7;
		long sz = c & 15;
		int shift = 4;
		int p = 1;
		while ((c & 0x80) != 0) {
			c = ib[p++] & 0xff;
			sz += ((long) (c & 0x7f)) << shift;
			shift += 7;
		}

		long deltaAt;
		switch (type) {
		case Constants.OBJ_COMMIT:
		case Constants.OBJ_TREE:
		case Constants.OBJ_BLOB:
		case Constants.OBJ_TAG:
			return sz;

		case Constants.OBJ_OFS_DELTA:
			c = ib[p++] & 0xff;
			while ((c & 128) != 0)
				c = ib[p++] & 0xff;
			deltaAt = pos + p;
			break;

		case Constants.OBJ_REF_DELTA:
			deltaAt = pos + p + 20;
			break;

		default:
			throw new IOException(MessageFormat.format(
					JGitText.get().unknownObjectType, Integer.valueOf(type)));
		}

		try {
			return BinaryDelta.getResultSize(getDeltaHeader(curs, deltaAt));
		} catch (DataFormatException e) {
			throw new CorruptObjectException(MessageFormat.format(
					JGitText.get().objectAtHasBadZlibStream, Long.valueOf(pos),
					getPackFile()));
		}
	}

	@Override
	LocalObjectRepresentation representation(final WindowCursor curs,
			final AnyObjectId objectId) throws IOException {
		final long pos = idx().findOffset(objectId);
		if (pos < 0)
			return null;

		final byte[] ib = curs.tempId;
		readFully(pos, ib, 0, 20, curs);
		int c = ib[0] & 0xff;
		int p = 1;
		final int typeCode = (c >> 4) & 7;
		while ((c & 0x80) != 0)
			c = ib[p++] & 0xff;

		long len = (findEndOffset(pos) - pos);
		switch (typeCode) {
		case Constants.OBJ_COMMIT:
		case Constants.OBJ_TREE:
		case Constants.OBJ_BLOB:
		case Constants.OBJ_TAG:
			return LocalObjectRepresentation.newWhole(this, pos, len - p);

		case Constants.OBJ_OFS_DELTA: {
			c = ib[p++] & 0xff;
			long ofs = c & 127;
			while ((c & 128) != 0) {
				ofs += 1;
				c = ib[p++] & 0xff;
				ofs <<= 7;
				ofs += (c & 127);
			}
			ofs = pos - ofs;
			return LocalObjectRepresentation.newDelta(this, pos, len - p, ofs);
		}

		case Constants.OBJ_REF_DELTA: {
			len -= p;
			len -= Constants.OBJECT_ID_LENGTH;
			readFully(pos + p, ib, 0, 20, curs);
			ObjectId id = ObjectId.fromRaw(ib);
			return LocalObjectRepresentation.newDelta(this, pos, len, id);
		}

		default:
			throw new IOException(
					MessageFormat.format(JGitText.get().unknownObjectType,
							Integer.valueOf(typeCode)));
		}
	}

	private long findEndOffset(long startOffset)
			throws IOException, CorruptObjectException {
		final long maxOffset = length - 20;
		return getReverseIdx().findNextOffset(startOffset, maxOffset);
	}

	@Override
	synchronized PackBitmapIndex getBitmapIndex() throws IOException {
		if (invalid || invalidBitmap)
			return null;
		if (bitmapIdx == null && hasExt(BITMAP_INDEX)) {
			final PackBitmapIndex idx;
			try {
				idx = PackBitmapIndex.open(extFile(BITMAP_INDEX), idx(),
						getReverseIdx());
			} catch (FileNotFoundException e) {
				// Once upon a time this bitmap file existed. Now it
				// has been removed. Most likely an external gc has
				// removed this packfile and the bitmap
				invalidBitmap = true;
				return null;
			}

			// At this point, idx() will have set packChecksum.
			if (Arrays.equals(packChecksum, idx.packChecksum))
				bitmapIdx = idx;
			else
				invalidBitmap = true;
		}
		return bitmapIdx;
	}

	private synchronized PackReverseIndex getReverseIdx() throws IOException {
		if (reverseIdx == null)
			reverseIdx = new PackReverseIndex(idx());
		return reverseIdx;
	}

	private boolean isCorrupt(long offset) {
		LongList list = corruptObjects;
		if (list == null)
			return false;
		synchronized (list) {
			return list.contains(offset);
		}
	}

	private ByteArrayFile extFile(PackExt ext) {
		String p = packFile.getName();
		int dot = p.lastIndexOf('.');
		String b = (dot < 0) ? p : p.substring(0, dot);
		// CHECKME
		return new ByteArrayFile(packFile.getParentFile(),
				b + '.' + ext.getExtension());
	}

	private boolean hasExt(PackExt ext) {
		return (extensions & ext.getBit()) != 0;
	}
}

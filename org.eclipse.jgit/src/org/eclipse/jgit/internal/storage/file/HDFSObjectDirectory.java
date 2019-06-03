package org.eclipse.jgit.internal.storage.file;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.eclipse.jgit.internal.storage.pack.PackExt.INDEX;
import static org.eclipse.jgit.internal.storage.pack.PackExt.PACK;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.jgit.errors.CorruptObjectException;
import org.eclipse.jgit.errors.PackInvalidException;
import org.eclipse.jgit.errors.PackMismatchException;
import org.eclipse.jgit.internal.JGitText;
import org.eclipse.jgit.internal.storage.pack.ObjectToPack;
import org.eclipse.jgit.internal.storage.pack.PackExt;
import org.eclipse.jgit.internal.storage.pack.PackWriter;
import org.eclipse.jgit.lib.AbbreviatedObjectId;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.Config;
//import org.eclipse.jgit.lib.ConfigConstants;
//import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectDatabase;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.RepositoryCache;
import org.eclipse.jgit.lib.RepositoryCache.FileKey;
import org.eclipse.jgit.util.FS;
import org.eclipse.jgit.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hyj
 *
 */
public class HDFSObjectDirectory extends FileObjectDatabase {
	private final static Logger LOG = LoggerFactory
			.getLogger(HDFSObjectDirectory.class);

	private static final PackList NO_PACKS = new PackList(
			HDFSFileSnapshot.DIRTY,
			new HDFSPackFile[0]);

	/** Maximum number of candidates offered as resolutions of abbreviation. */
	// private static final int RESOLVE_ABBREV_LIMIT = 256;
	private final AlternateHandle handle = new AlternateHandle(this);
	private final Config config;

	// TODO use HDFSFIle
	private final File objects;
	private final File infoDirectory;
	private final File packDirectory;
	private final File preservedDirectory;
	private final File alternatesFile;

	// CHECKME FS?
	private final FS fs;

	private final AtomicReference<AlternateHandle[]> alternates;
	private final UnpackedObjectCache unpackedObjectCache;
	private final File shallowFile;

	// CHANGED use HDFSFileSnapshot
	private HDFSFileSnapshot shallowFileSnapshot = HDFSFileSnapshot.DIRTY;

	private Set<ObjectId> shallowCommitsIds;
	final AtomicReference<PackList> packList;

	/**
	 * @param cfg
	 * @param dir
	 * @param alternatePaths
	 * @param fs
	 * @param shallowFile
	 * @throws IOException
	 */
	// TODO use HDFSFile
	public HDFSObjectDirectory(final Config cfg, final File dir,
			File[] alternatePaths, FS fs, File shallowFile) throws IOException {
		config = cfg;
		objects = dir;
		infoDirectory = new File(objects, "info"); //$NON-NLS-1$
		packDirectory = new File(objects, "pack"); //$NON-NLS-1$
		preservedDirectory = new File(packDirectory, "preserved"); //$NON-NLS-1$
		alternatesFile = new File(infoDirectory, "alternates"); //$NON-NLS-1$
		packList = new AtomicReference<>(NO_PACKS);
		unpackedObjectCache = new UnpackedObjectCache();
		this.fs = fs;
		this.shallowFile = shallowFile;

		alternates = new AtomicReference<>();
		if (alternatePaths != null) {
			AlternateHandle[] alt;

			alt = new AlternateHandle[alternatePaths.length];
			for (int i = 0; i < alternatePaths.length; i++)
				alt[i] = openAlternate(alternatePaths[i]);
			alternates.set(alt);
		}
	}

	/** {@inheritDoc} */
	@Override
	public final File getDirectory() {
		return objects;
	}

	/**
	 * <p>
	 * Getter for the field <code>packDirectory</code>.
	 * </p>
	 *
	 * @return the location of the <code>pack</code> directory.
	 */
	public final File getPackDirectory() {
		return packDirectory;
	}

	/**
	 * <p>
	 * Getter for the field <code>preservedDirectory</code>.
	 * </p>
	 *
	 * @return the location of the <code>preserved</code> directory.
	 */
	public final File getPreservedDirectory() {
		return preservedDirectory;
	}

	/** {@inheritDoc} */
	@Override
	public boolean exists() {
		return fs.exists(objects);
	}

	/** {@inheritDoc} */
	@Override
	public void create() throws IOException {
		// IGNOREME
	}

	/** {@inheritDoc} */
	@Override
	public ObjectDirectoryInserter newInserter() {
		return new ObjectDirectoryInserter(this, config);
	}

	/** {@inheritDoc} */
	@Override
	public void close() {
		unpackedObjectCache.clear();

		final PackList packs = packList.get();
		if (packs != NO_PACKS && packList.compareAndSet(packs, NO_PACKS)) {
			for (HDFSPackFile p : packs.packs)
				p.close();
		}

		// Fully close all loaded alternates and clear the alternate list.
		AlternateHandle[] alt = alternates.get();
		if (alt != null && alternates.compareAndSet(alt, null)) {
			for (AlternateHandle od : alt)
				od.close();
		}
	}

	/** {@inheritDoc} */
	@Override
	public Collection<PackFile> getPacks() {
		PackList list = packList.get();
		if (list == NO_PACKS)
			list = scanPacks(list);
		HDFSPackFile[] packs = list.packs;
		return Collections.unmodifiableCollection(Arrays.asList(packs));
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Add a single existing pack to the list of available pack files.
	 */
	@Override
	public HDFSPackFile openPack(File pack) throws IOException {
		final String p = pack.getName();
		if (p.length() != 50 || !p.startsWith("pack-") || !p.endsWith(".pack")) //$NON-NLS-1$ //$NON-NLS-2$
			throw new IOException(
					MessageFormat.format(JGitText.get().notAValidPack, pack));

		// The pack and index are assumed to exist. The existence of other
		// extensions needs to be explicitly checked.
		//
		int extensions = PACK.getBit() | INDEX.getBit();
		final String base = p.substring(0, p.length() - 4);
		for (PackExt ext : PackExt.values()) {
			if ((extensions & ext.getBit()) == 0) {
				final String name = base + ext.getExtension();
				if (new File(pack.getParentFile(), name).exists())
					extensions |= ext.getBit();
			}
		}

		HDFSPackFile res = new HDFSPackFile(pack, extensions);
		insertPack(res);
		return res;
	}

	/** {@inheritDoc} */
	@Override
	public String toString() {
		return "HDFSObjectDirectory[" + getDirectory() + "]"; //$NON-NLS-1$ //$NON-NLS-2$
	}

	/** {@inheritDoc} */
	@Override
	public boolean has(AnyObjectId objectId) {
		return unpackedObjectCache.isUnpacked(objectId)
				|| hasPackedInSelfOrAlternate(objectId, null)
				|| hasLooseInSelfOrAlternate(objectId, null);
	}

	private boolean hasPackedInSelfOrAlternate(AnyObjectId objectId,
			Set<AlternateHandle.Id> skips) {
		if (hasPackedObject(objectId)) {
			return true;
		}
		skips = addMe(skips);
		for (AlternateHandle alt : myAlternates()) {
			if (!skips.contains(alt.getId())) {
				if (alt.db.hasPackedInSelfOrAlternate(objectId, skips)) {
					return true;
				}
			}
		}
		return false;
	}

	private boolean hasLooseInSelfOrAlternate(AnyObjectId objectId,
			Set<AlternateHandle.Id> skips) {
		if (fileFor(objectId).exists()) {
			return true;
		}
		skips = addMe(skips);
		for (AlternateHandle alt : myAlternates()) {
			if (!skips.contains(alt.getId())) {
				if (alt.db.hasLooseInSelfOrAlternate(objectId, skips)) {
					return true;
				}
			}
		}
		return false;
	}

	boolean hasPackedObject(AnyObjectId objectId) {
		PackList pList;
		do {
			pList = packList.get();
			for (HDFSPackFile p : pList.packs) {
				try {
					if (p.hasObject(objectId))
						return true;
				} catch (IOException e) {
					// The hasObject call should have only touched the index,
					// so any failure here indicates the index is unreadable
					// by this process, and the pack is likewise not readable.
					LOG.warn(MessageFormat.format(
							JGitText.get().unableToReadPackfile,
							p.getPackFile().getAbsolutePath()), e);
					removePack(p);
				}
			}
		} while (searchPacksAgain(pList));
		return false;
	}

	@Override
	void resolve(Set<ObjectId> matches, AbbreviatedObjectId id)
			throws IOException {
		// IGNOREME
		// resolve(matches, id, null);
	}

	// private void resolve(Set<ObjectId> matches, AbbreviatedObjectId id,
	// Set<AlternateHandle.Id> skips) throws IOException {
	// // Go through the packs once. If we didn't find any resolutions
	// // scan for new packs and check once more.
	// int oldSize = matches.size();
	// PackList pList;
	// do {
	// pList = packList.get();
	// for (HDFSPackFile p : pList.packs) {
	// try {
	// p.resolve(matches, id, RESOLVE_ABBREV_LIMIT);
	// p.resetTransientErrorCount();
	// } catch (IOException e) {
	// handlePackError(e, p);
	// }
	// if (matches.size() > RESOLVE_ABBREV_LIMIT)
	// return;
	// }
	// } while (matches.size() == oldSize && searchPacksAgain(pList));
	//
	// String fanOut = id.name().substring(0, 2);
	// String[] entries = new File(getDirectory(), fanOut).list();
	// if (entries != null) {
	// for (String e : entries) {
	// if (e.length() != Constants.OBJECT_ID_STRING_LENGTH - 2)
	// continue;
	// try {
	// ObjectId entId = ObjectId.fromString(fanOut + e);
	// if (id.prefixCompare(entId) == 0)
	// matches.add(entId);
	// } catch (IllegalArgumentException notId) {
	// continue;
	// }
	// if (matches.size() > RESOLVE_ABBREV_LIMIT)
	// return;
	// }
	// }
	//
	// skips = addMe(skips);
	// for (AlternateHandle alt : myAlternates()) {
	// if (!skips.contains(alt.getId())) {
	// alt.db.resolve(matches, id, skips);
	// if (matches.size() > RESOLVE_ABBREV_LIMIT) {
	// return;
	// }
	// }
	// }
	// }

	@Override
	ObjectLoader openObject(WindowCursor curs, AnyObjectId objectId)
			throws IOException {
		// IGNOREME
		// if (unpackedObjectCache.isUnpacked(objectId)) {
		// ObjectLoader ldr = openLooseObject(curs, objectId);
		// if (ldr != null) {
		// return ldr;
		// }
		// }
		ObjectLoader ldr = openPackedFromSelfOrAlternate(curs, objectId, null);
		if (ldr != null) {
			return ldr;
		}
		return null;
		// IGNOREME
		// return openLooseFromSelfOrAlternate(curs, objectId, null);
	}

	private ObjectLoader openPackedFromSelfOrAlternate(WindowCursor curs,
			AnyObjectId objectId, Set<AlternateHandle.Id> skips) {
		ObjectLoader ldr = openPackedObject(curs, objectId);
		if (ldr != null) {
			return ldr;
		}
		// IGNOREME
		// skips = addMe(skips);
		// for (AlternateHandle alt : myAlternates()) {
		// if (!skips.contains(alt.getId())) {
		// ldr = alt.db.openPackedFromSelfOrAlternate(curs, objectId,
		// skips);
		// if (ldr != null) {
		// return ldr;
		// }
		// }
		// }
		return null;
	}

	// private ObjectLoader openLooseFromSelfOrAlternate(WindowCursor curs,
	// AnyObjectId objectId, Set<AlternateHandle.Id> skips)
	// throws IOException {
	// ObjectLoader ldr = openLooseObject(curs, objectId);
	// if (ldr != null) {
	// return ldr;
	// }
	// skips = addMe(skips);
	// for (AlternateHandle alt : myAlternates()) {
	// if (!skips.contains(alt.getId())) {
	// ldr = alt.db.openLooseFromSelfOrAlternate(curs, objectId,
	// skips);
	// if (ldr != null) {
	// return ldr;
	// }
	// }
	// }
	// return null;
	// }

	ObjectLoader openPackedObject(WindowCursor curs, AnyObjectId objectId) {
		PackList pList;
		do {
			SEARCH: for (;;) {
				pList = packList.get();
				System.out.println("openPackedObject 1 " + pList.packs.length);
				for (HDFSPackFile p : pList.packs) {
					try {
						System.out.println(
								"openPackedObject 1.5 " + p.getPackName());
						ObjectLoader ldr = p.get(curs, objectId);
						p.resetTransientErrorCount();
						System.out.println("openPackedObject 2");
						if (ldr != null)
							return ldr;
					} catch (PackMismatchException e) {
						// Pack was modified; refresh the entire pack list.
						System.out.println("openPackedObject 3");
						if (searchPacksAgain(pList))
							continue SEARCH;
					} catch (IOException e) {
						handlePackError(e, p);
					}
				}
				break SEARCH;
			}
		} while (searchPacksAgain(pList));
		return null;
	}

	@Override
	ObjectLoader openLooseObject(WindowCursor curs, AnyObjectId id)
			throws IOException {
		File path = fileFor(id);
		try (FileInputStream in = new FileInputStream(path)) {
			unpackedObjectCache.add(id);
			return UnpackedObject.open(in, path, id, curs);
		} catch (FileNotFoundException noFile) {
			if (path.exists()) {
				throw noFile;
			}
			unpackedObjectCache.remove(id);
			return null;
		}
	}

	@Override
	long getObjectSize(WindowCursor curs, AnyObjectId id) throws IOException {
		if (unpackedObjectCache.isUnpacked(id)) {
			long len = getLooseObjectSize(curs, id);
			if (0 <= len) {
				return len;
			}
		}
		long len = getPackedSizeFromSelfOrAlternate(curs, id, null);
		if (0 <= len) {
			return len;
		}
		return getLooseSizeFromSelfOrAlternate(curs, id, null);
	}

	private long getPackedSizeFromSelfOrAlternate(WindowCursor curs,
			AnyObjectId id, Set<AlternateHandle.Id> skips) {
		long len = getPackedObjectSize(curs, id);
		if (0 <= len) {
			return len;
		}
		skips = addMe(skips);
		for (AlternateHandle alt : myAlternates()) {
			if (!skips.contains(alt.getId())) {
				len = alt.db.getPackedSizeFromSelfOrAlternate(curs, id, skips);
				if (0 <= len) {
					return len;
				}
			}
		}
		return -1;
	}

	private long getLooseSizeFromSelfOrAlternate(WindowCursor curs,
			AnyObjectId id, Set<AlternateHandle.Id> skips) throws IOException {
		long len = getLooseObjectSize(curs, id);
		if (0 <= len) {
			return len;
		}
		skips = addMe(skips);
		for (AlternateHandle alt : myAlternates()) {
			if (!skips.contains(alt.getId())) {
				len = alt.db.getLooseSizeFromSelfOrAlternate(curs, id, skips);
				if (0 <= len) {
					return len;
				}
			}
		}
		return -1;
	}

	private long getPackedObjectSize(WindowCursor curs, AnyObjectId id) {
		PackList pList;
		do {
			SEARCH: for (;;) {
				pList = packList.get();
				for (HDFSPackFile p : pList.packs) {
					try {
						long len = p.getObjectSize(curs, id);
						p.resetTransientErrorCount();
						if (0 <= len)
							return len;
					} catch (PackMismatchException e) {
						// Pack was modified; refresh the entire pack list.
						if (searchPacksAgain(pList))
							continue SEARCH;
					} catch (IOException e) {
						handlePackError(e, p);
					}
				}
				break SEARCH;
			}
		} while (searchPacksAgain(pList));
		return -1;
	}

	private long getLooseObjectSize(WindowCursor curs, AnyObjectId id)
			throws IOException {
		File f = fileFor(id);
		try (FileInputStream in = new FileInputStream(f)) {
			unpackedObjectCache.add(id);
			return UnpackedObject.getSize(in, id, curs);
		} catch (FileNotFoundException noFile) {
			if (f.exists()) {
				throw noFile;
			}
			unpackedObjectCache.remove(id);
			return -1;
		}
	}

	@Override
	void selectObjectRepresentation(PackWriter packer, ObjectToPack otp,
			WindowCursor curs) throws IOException {
		selectObjectRepresentation(packer, otp, curs, null);
	}

	private void selectObjectRepresentation(PackWriter packer, ObjectToPack otp,
			WindowCursor curs, Set<AlternateHandle.Id> skips)
			throws IOException {
		PackList pList = packList.get();
		SEARCH: for (;;) {
			for (HDFSPackFile p : pList.packs) {
				try {
					LocalObjectRepresentation rep = p.representation(curs, otp);
					p.resetTransientErrorCount();
					if (rep != null)
						packer.select(otp, rep);
				} catch (PackMismatchException e) {
					// Pack was modified; refresh the entire pack list.
					//
					pList = scanPacks(pList);
					continue SEARCH;
				} catch (IOException e) {
					handlePackError(e, p);
				}
			}
			break SEARCH;
		}

		skips = addMe(skips);
		for (AlternateHandle h : myAlternates()) {
			if (!skips.contains(h.getId())) {
				h.db.selectObjectRepresentation(packer, otp, curs, skips);
			}
		}
	}

	private void handlePackError(IOException e, HDFSPackFile p) {
		String warnTmpl = null;
		int transientErrorCount = 0;
		String errTmpl = JGitText.get().exceptionWhileReadingPack;
		if ((e instanceof CorruptObjectException)
				|| (e instanceof PackInvalidException)) {
			warnTmpl = JGitText.get().corruptPack;
			LOG.warn(MessageFormat.format(warnTmpl,
					p.getPackFile().getAbsolutePath()), e);
			// Assume the pack is corrupted, and remove it from the list.
			removePack(p);
		} else if (e instanceof FileNotFoundException) {
			if (p.getPackFile().exists()) {
				errTmpl = JGitText.get().packInaccessible;
				transientErrorCount = p.incrementTransientErrorCount();
			} else {
				warnTmpl = JGitText.get().packWasDeleted;
				removePack(p);
			}
		} else if (FileUtils.isStaleFileHandleInCausalChain(e)) {
			warnTmpl = JGitText.get().packHandleIsStale;
			removePack(p);
		} else {
			transientErrorCount = p.incrementTransientErrorCount();
		}
		if (warnTmpl != null) {
			LOG.warn(MessageFormat.format(warnTmpl,
					p.getPackFile().getAbsolutePath()), e);
		} else {
			if (doLogExponentialBackoff(transientErrorCount)) {
				// Don't remove the pack from the list, as the error may be
				// transient.
				LOG.error(MessageFormat.format(errTmpl,
						p.getPackFile().getAbsolutePath(),
						Integer.valueOf(transientErrorCount)), e);
			}
		}
	}

	/**
	 * @param n
	 *            count of consecutive failures
	 * @return @{code true} if i is a power of 2
	 */
	private boolean doLogExponentialBackoff(int n) {
		return (n & (n - 1)) == 0;
	}

	@Override
	InsertLooseObjectResult insertUnpackedObject(File tmp, ObjectId id,
			boolean createDuplicate) throws IOException {
		// If the object is already in the repository, remove temporary file.
		//
		if (unpackedObjectCache.isUnpacked(id)) {
			FileUtils.delete(tmp, FileUtils.RETRY);
			return InsertLooseObjectResult.EXISTS_LOOSE;
		}
		if (!createDuplicate && has(id)) {
			FileUtils.delete(tmp, FileUtils.RETRY);
			return InsertLooseObjectResult.EXISTS_PACKED;
		}

		final File dst = fileFor(id);
		if (dst.exists()) {
			// We want to be extra careful and avoid replacing an object
			// that already exists. We can't be sure renameTo() would
			// fail on all platforms if dst exists, so we check first.
			//
			FileUtils.delete(tmp, FileUtils.RETRY);
			return InsertLooseObjectResult.EXISTS_LOOSE;
		}
		try {
			Files.move(FileUtils.toPath(tmp), FileUtils.toPath(dst),
					StandardCopyOption.ATOMIC_MOVE);
			dst.setReadOnly();
			unpackedObjectCache.add(id);
			return InsertLooseObjectResult.INSERTED;
		} catch (AtomicMoveNotSupportedException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			// ignore
		}

		// Maybe the directory doesn't exist yet as the object
		// directories are always lazily created. Note that we
		// try the rename first as the directory likely does exist.
		//
		FileUtils.mkdir(dst.getParentFile(), true);
		try {
			Files.move(FileUtils.toPath(tmp), FileUtils.toPath(dst),
					StandardCopyOption.ATOMIC_MOVE);
			dst.setReadOnly();
			unpackedObjectCache.add(id);
			return InsertLooseObjectResult.INSERTED;
		} catch (AtomicMoveNotSupportedException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			LOG.debug(e.getMessage(), e);
		}

		if (!createDuplicate && has(id)) {
			FileUtils.delete(tmp, FileUtils.RETRY);
			return InsertLooseObjectResult.EXISTS_PACKED;
		}

		// The object failed to be renamed into its proper
		// location and it doesn't exist in the repository
		// either. We really don't know what went wrong, so
		// fail.
		//
		FileUtils.delete(tmp, FileUtils.RETRY);
		return InsertLooseObjectResult.FAILURE;
	}

	boolean searchPacksAgain(PackList old) {
		// Whether to trust the pack folder's modification time. If set
		// to false we will always scan the .git/objects/pack folder to
		// check for new pack files. If set to true (default) we use the
		// lastmodified attribute of the folder and assume that no new
		// pack files can be in this folder if his modification time has
		// not changed.

		// IGNOREME
//		boolean trustFolderStat = config.getBoolean(
//				ConfigConstants.CONFIG_CORE_SECTION,
//				ConfigConstants.CONFIG_KEY_TRUSTFOLDERSTAT, true);

		// return ((!trustFolderStat) || old.snapshot.isModified(packDirectory))
		// && old != scanPacks(old);
		// boolean b1 = old.snapshot.isModified(packDirectory);

		boolean b2 = old != scanPacks(old);
		// System.out.println("searchPacksAgain " + b1 + " " + b2);
		// return b1 && b2;
		return b2;
	}

	@Override
	Config getConfig() {
		return config;
	}

	@Override
	FS getFS() {
		return fs;
	}

	@Override
	Set<ObjectId> getShallowCommits() throws IOException {
		if (shallowFile == null || !shallowFile.isFile())
			return Collections.emptySet();

		if (shallowFileSnapshot == null
				|| shallowFileSnapshot.isModified(shallowFile)) {
			shallowCommitsIds = new HashSet<>();

			try (BufferedReader reader = open(shallowFile)) {
				String line;
				while ((line = reader.readLine()) != null) {
					try {
						shallowCommitsIds.add(ObjectId.fromString(line));
					} catch (IllegalArgumentException ex) {
						throw new IOException(MessageFormat
								.format(JGitText.get().badShallowLine, line));
					}
				}
			}

			shallowFileSnapshot = HDFSFileSnapshot.save(shallowFile);
		}

		return shallowCommitsIds;
	}

	private void insertPack(HDFSPackFile pf) {
		PackList o, n;
		do {
			o = packList.get();

			// If the pack in question is already present in the list
			// (picked up by a concurrent thread that did a scan?) we
			// do not want to insert it a second time.
			//
			final HDFSPackFile[] oldList = o.packs;
			final String name = pf.getPackFile().getName();
			for (HDFSPackFile p : oldList) {
				if (name.equals(p.getPackFile().getName()))
					return;
			}

			final HDFSPackFile[] newList = new HDFSPackFile[1 + oldList.length];
			newList[0] = pf;
			System.arraycopy(oldList, 0, newList, 1, oldList.length);
			n = new PackList(o.snapshot, newList);
		} while (!packList.compareAndSet(o, n));
	}

	private void removePack(HDFSPackFile deadPack) {
		PackList o, n;
		do {
			o = packList.get();

			final HDFSPackFile[] oldList = o.packs;
			final int j = indexOf(oldList, deadPack);
			if (j < 0)
				break;

			final HDFSPackFile[] newList = new HDFSPackFile[oldList.length - 1];
			System.arraycopy(oldList, 0, newList, 0, j);
			System.arraycopy(oldList, j + 1, newList, j, newList.length - j);
			n = new PackList(o.snapshot, newList);
		} while (!packList.compareAndSet(o, n));
		deadPack.close();
	}

	private static int indexOf(HDFSPackFile[] list, HDFSPackFile pack) {
		for (int i = 0; i < list.length; i++) {
			if (list[i] == pack)
				return i;
		}
		return -1;
	}

	private PackList scanPacks(PackList original) {
		synchronized (packList) {
			PackList o, n;
			do {
				o = packList.get();
				if (o != original) {
					// Another thread did the scan for us, while we
					// were blocked on the monitor above.
					//
					return o;
				}
				n = scanPacksImpl(o);
				if (n == o)
					return n;
			} while (!packList.compareAndSet(o, n));
			return n;
		}
	}

	// TODO
	private PackList scanPacksImpl(PackList old) {
		final Map<String, HDFSPackFile> forReuse = reuseMap(old);
		// snapshot
		final HDFSFileSnapshot snapshot = HDFSFileSnapshot.save(packDirectory);
		final Set<String> names = listPackDirectory();
		final List<HDFSPackFile> list = new ArrayList<>(names.size() >> 2);
		boolean foundNew = false;

		System.out.println("1");

		for (String indexName : names) {
			System.out.println("2 " + indexName);

			// Must match "pack-[0-9a-f]{40}.idx" to be an index.
			//
			if (indexName.length() != 49 || !indexName.endsWith(".idx")) //$NON-NLS-1$
				continue;

			final String base = indexName.substring(0, indexName.length() - 3);
			int extensions = 0;
			for (PackExt ext : PackExt.values()) {
				if (names.contains(base + ext.getExtension()))
					extensions |= ext.getBit();
			}

			if ((extensions & PACK.getBit()) == 0) {
				// Sometimes C Git's HTTP fetch transport leaves a
				// .idx file behind and does not download the .pack.
				// We have to skip over such useless indexes.
				//
				continue;
			}

			final String packName = base + PACK.getExtension();
			final File packFile = new File(packDirectory, packName);
			// if cannot find the oid
			final HDFSPackFile oldPack = forReuse.remove(packName);

			boolean b1 = oldPack != null;
			boolean b2 = oldPack != null
					&& !oldPack.getFileSnapshot().isModified(packFile);

			System.out.println(b1 + " " + b2);

			if (b2) {
				System.out.println("2.3");
				list.add(oldPack);
				continue;
			}
			System.out.println("2.5");
			list.add(new HDFSPackFile(packFile, extensions));
			foundNew = true;
		}

		// If we did not discover any new files, the modification time was not
		// changed, and we did not remove any files, then the set of files is
		// the same as the set we were given. Instead of building a new object
		// return the same collection.
		//
		// snapshot
		if (!foundNew && forReuse.isEmpty() && snapshot.equals(old.snapshot)) {
			old.snapshot.setClean(snapshot);
			System.out.println("3");
			return old;
		}

		for (HDFSPackFile p : forReuse.values()) {
			System.out.println("4");
			p.close();
		}

		if (list.isEmpty())
			return new PackList(snapshot, NO_PACKS.packs);

		System.out
				.println("5 " + list.size() + " " + list.get(0).getPackName());

		final HDFSPackFile[] r = list.toArray(new HDFSPackFile[0]);
		Arrays.sort(r, HDFSPackFile.SORT);
		return new PackList(snapshot, r);
	}

	private static Map<String, HDFSPackFile> reuseMap(PackList old) {
		final Map<String, HDFSPackFile> forReuse = new HashMap<>();
		for (HDFSPackFile p : old.packs) {
			if (p.invalid()) {
				// The pack instance is corrupted, and cannot be safely used
				// again. Do not include it in our reuse map.
				//
				p.close();
				continue;
			}

			final HDFSPackFile prior = forReuse.put(p.getPackFile().getName(), p);
			if (prior != null) {
				// This should never occur. It should be impossible for us
				// to have two pack files with the same name, as all of them
				// came out of the same directory. If it does, we promised to
				// close any PackFiles we did not reuse, so close the second,
				// readers are likely to be actively using the first.
				//
				forReuse.put(prior.getPackFile().getName(), prior);
				p.close();
			}
		}
		return forReuse;
	}

	private Set<String> listPackDirectory() {
		final String[] nameList = packDirectory.list();
		if (nameList == null)
			return Collections.emptySet();
		final Set<String> nameSet = new HashSet<>(nameList.length << 1);
		for (String name : nameList) {
			if (name.startsWith("pack-")) //$NON-NLS-1$
				nameSet.add(name);
		}
		return nameSet;
	}

	void closeAllPackHandles(File packFile) {
		// if the packfile already exists (because we are rewriting a
		// packfile for the same set of objects maybe with different
		// PackConfig) then make sure we get rid of all handles on the file.
		// Windows will not allow for rename otherwise.
		if (packFile.exists()) {
			for (PackFile p : getPacks()) {
				if (packFile.getPath().equals(p.getPackFile().getPath())) {
					p.close();
					break;
				}
			}
		}
	}

	AlternateHandle[] myAlternates() {
		AlternateHandle[] alt = alternates.get();
		if (alt == null) {
			synchronized (alternates) {
				alt = alternates.get();
				if (alt == null) {
					try {
						alt = loadAlternates();
					} catch (IOException e) {
						alt = new AlternateHandle[0];
					}
					alternates.set(alt);
				}
			}
		}
		return alt;
	}

	Set<AlternateHandle.Id> addMe(Set<AlternateHandle.Id> skips) {
		if (skips == null) {
			skips = new HashSet<>();
		}
		skips.add(handle.getId());
		return skips;
	}

	private AlternateHandle[] loadAlternates() throws IOException {
		final List<AlternateHandle> l = new ArrayList<>(4);
		try (BufferedReader br = open(alternatesFile)) {
			String line;
			while ((line = br.readLine()) != null) {
				l.add(openAlternate(line));
			}
		}
		return l.toArray(new AlternateHandle[0]);
	}

	private static BufferedReader open(File f)
			throws IOException, FileNotFoundException {
		return Files.newBufferedReader(f.toPath(), UTF_8);
	}

	private AlternateHandle openAlternate(String location) throws IOException {
		final File objdir = fs.resolve(objects, location);
		return openAlternate(objdir);
	}

	private AlternateHandle openAlternate(File objdir) throws IOException {
		final File parent = objdir.getParentFile();
		if (FileKey.isGitRepository(parent, fs)) {
			FileKey key = FileKey.exact(parent, fs);
			HDFSFileRepository db = (HDFSFileRepository) RepositoryCache
					.open(key);
			return new AlternateRepository(db);
		}

		HDFSObjectDirectory db = new HDFSObjectDirectory(config, objdir, null,
				fs,
				null);
		return new AlternateHandle(db);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Compute the location of a loose object file.
	 */
	@Override
	public File fileFor(AnyObjectId objectId) {
		String n = objectId.name();
		String d = n.substring(0, 2);
		String f = n.substring(2);
		return new File(new File(getDirectory(), d), f);
	}

	static final class PackList {
		/** State just before reading the pack directory. */
		final HDFSFileSnapshot snapshot;

		/** All known packs, sorted by {@link HDFSPackFile#SORT}. */
		final HDFSPackFile[] packs;

		PackList(HDFSFileSnapshot monitor, HDFSPackFile[] packs) {
			this.snapshot = monitor;
			this.packs = packs;
		}
	}

	static class AlternateHandle {
		static class Id {
			String alternateId;

			public Id(File object) {
				try {
					this.alternateId = object.getCanonicalPath();
				} catch (Exception e) {
					alternateId = null;
				}
			}

			@Override
			public boolean equals(Object o) {
				if (o == this) {
					return true;
				}
				if (o == null || !(o instanceof Id)) {
					return false;
				}
				Id aId = (Id) o;
				return Objects.equals(alternateId, aId.alternateId);
			}

			@Override
			public int hashCode() {
				if (alternateId == null) {
					return 1;
				}
				return alternateId.hashCode();
			}
		}

		final HDFSObjectDirectory db;

		AlternateHandle(HDFSObjectDirectory db) {
			this.db = db;
		}

		void close() {
			db.close();
		}

		public Id getId() {
			return db.getAlternateId();
		}
	}

	static class AlternateRepository extends AlternateHandle {
		final HDFSFileRepository repository;

		AlternateRepository(HDFSFileRepository r) {
			super(r.getObjectDatabase());
			repository = r;
		}

		@Override
		void close() {
			repository.close();
		}
	}

	/** {@inheritDoc} */
	@Override
	public ObjectDatabase newCachedDatabase() {
		return newCachedFileObjectDatabase();
	}

	HDFSCachedObjectDirectory newCachedFileObjectDatabase() {
		return new HDFSCachedObjectDirectory(this);
	}

	AlternateHandle.Id getAlternateId() {
		return new AlternateHandle.Id(objects);
	}
}

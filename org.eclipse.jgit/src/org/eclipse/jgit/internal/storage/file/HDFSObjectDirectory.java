package org.eclipse.jgit.internal.storage.file;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.eclipse.jgit.internal.storage.pack.PackExt.PACK;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
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
			FileSnapshot.DIRTY,
			new HDFSPackFile[0]);

	private final AlternateHandle handle = new AlternateHandle(this);
	private final Config config;

	private final HDFSFile objects;
	private final HDFSFile infoDirectory;
	private final HDFSFile packDirectory;
	private final HDFSFile preservedDirectory;
	private final HDFSFile alternatesFile;

	private final FS fs;

	private final AtomicReference<AlternateHandle[]> alternates;
	private final UnpackedObjectCache unpackedObjectCache;
	private final HDFSFile shallowFile;

	private FileSnapshot shallowFileSnapshot = FileSnapshot.DIRTY;

	private Set<ObjectId> shallowCommitsIds;
	final AtomicReference<PackList> packList;

	/**
	 * @param cfg
	 * @param dir
	 * @param fs
	 * @param shallowFile
	 * @throws IOException
	 */
	// TODO use HDFSFile
	public HDFSObjectDirectory(final Config cfg, final HDFSFile dir, FS fs,
			HDFSFile shallowFile) throws IOException {
		config = cfg;
		objects = dir;
		infoDirectory = new HDFSFile(objects, "info"); //$NON-NLS-1$
		packDirectory = new HDFSFile(objects, "pack"); //$NON-NLS-1$

		preservedDirectory = new HDFSFile(packDirectory, "preserved"); //$NON-NLS-1$
		alternatesFile = new HDFSFile(infoDirectory, "alternates"); //$NON-NLS-1$
		packList = new AtomicReference<>(NO_PACKS);
		unpackedObjectCache = new UnpackedObjectCache();
		this.fs = fs;
		this.shallowFile = shallowFile;

		alternates = new AtomicReference<>();
	}

	/** {@inheritDoc} */
	@Override
	public final HDFSFile getDirectory() {
		return objects;
	}

	/**
	 * <p>
	 * Getter for the field <code>packDirectory</code>.
	 * </p>
	 *
	 * @return the location of the <code>pack</code> directory.
	 */
	public final HDFSFile getPackDirectory() {
		return packDirectory;
	}

	/**
	 * <p>
	 * Getter for the field <code>preservedDirectory</code>.
	 * </p>
	 *
	 * @return the location of the <code>preserved</code> directory.
	 */
	public final HDFSFile getPreservedDirectory() {
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
		return (HDFSPackFile) (new Object());
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
	}

	@Override
	ObjectLoader openObject(WindowCursor curs, AnyObjectId objectId)
			throws IOException {
		ObjectLoader ldr = openPackedFromSelfOrAlternate(curs, objectId);
		if (ldr != null) {
			return ldr;
		}
		return null;
	}

	private ObjectLoader openPackedFromSelfOrAlternate(WindowCursor curs,
			AnyObjectId objectId) {
		ObjectLoader ldr = openPackedObject(curs, objectId);
		if (ldr != null) {
			return ldr;
		}
		return null;
	}

	ObjectLoader openPackedObject(WindowCursor curs, AnyObjectId objectId) {
		PackList pList;
		do {
			SEARCH: for (;;) {
				pList = packList.get();
				for (HDFSPackFile p : pList.packs) {
					try {
						ObjectLoader ldr = p.get(curs, objectId);
						p.resetTransientErrorCount();
						if (ldr != null)
							return ldr;
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
		return null;
	}

	@Override
	ObjectLoader openLooseObject(WindowCursor curs, AnyObjectId id)
			throws IOException {
		return null;
	}

	@Override
	long getObjectSize(WindowCursor curs, AnyObjectId id) throws IOException {
		return -1;
	}

	@Override
	void selectObjectRepresentation(PackWriter packer, ObjectToPack otp,
			WindowCursor curs) throws IOException {
		// IGNOREME
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
		return old != scanPacks(old);
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

			shallowFileSnapshot = FileSnapshot.save(shallowFile);
		}

		return shallowCommitsIds;
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
				n = scanPacksImpl(o); // CHECKME
				if (n == o)
					return n;
			} while (!packList.compareAndSet(o, n));
			return n;
		}
	}

	private PackList scanPacksImpl(PackList old) {
		final Map<String, HDFSPackFile> forReuse = reuseMap(old);
		// snapshot
		final FileSnapshot snapshot = FileSnapshot.save(packDirectory);
		final Set<String> names = listPackDirectory();
		final List<HDFSPackFile> list = new ArrayList<>(names.size() >> 2);
		boolean foundNew = false;

		for (String indexName : names) {

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
			final HDFSFile packFile = new HDFSFile(packDirectory, packName);
			// if cannot find the oid
			final HDFSPackFile oldPack = forReuse.remove(packName);

			if (oldPack != null
					&& !oldPack.getFileSnapshot().isModified(packFile)) {
				list.add(oldPack);
				continue;
			}
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
			return old;
		}

		for (HDFSPackFile p : forReuse.values()) {
			p.close();
		}

		if (list.isEmpty())
			return new PackList(snapshot, NO_PACKS.packs);

		final HDFSPackFile[] r = list.toArray(new HDFSPackFile[0]);
		Arrays.sort(r, PackFile.SORT);
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
			while (br.readLine() != null) {
				l.add(openAlternate());
			}
		}
		return l.toArray(new AlternateHandle[0]);
	}

	private static BufferedReader open(File f)
			throws IOException, FileNotFoundException {
		return Files.newBufferedReader(f.toPath(), UTF_8);
	}

	private AlternateHandle openAlternate() {
		return null;
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
		final FileSnapshot snapshot;

		/** All known packs, sorted by {@link HDFSPackFile#SORT}. */
		final HDFSPackFile[] packs;

		PackList(FileSnapshot monitor, HDFSPackFile[] packs) {
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
		return null;
	}

	AlternateHandle.Id getAlternateId() {
		return new AlternateHandle.Id(objects);
	}
}

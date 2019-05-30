package org.eclipse.jgit.internal.storage.file;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.eclipse.jgit.internal.storage.file.HDFSObjectDirectory.AlternateHandle;
import org.eclipse.jgit.internal.storage.pack.ObjectToPack;
import org.eclipse.jgit.internal.storage.pack.PackWriter;
import org.eclipse.jgit.lib.AbbreviatedObjectId;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.Config;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectDatabase;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectIdOwnerMap;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.util.FS;

/**
 * @author hyj
 *
 */
class HDFSCachedObjectDirectory extends FileObjectDatabase {
	/**
	 * The set that contains unpacked objects identifiers, it is created when
	 * the cached instance is created.
	 */
	private ObjectIdOwnerMap<UnpackedObjectId> unpackedObjects;

	private final HDFSObjectDirectory wrapped;

	private HDFSCachedObjectDirectory[] alts;

	/**
	 * The constructor
	 *
	 * @param wrapped
	 *            the wrapped database
	 */
	HDFSCachedObjectDirectory(HDFSObjectDirectory wrapped) {
		this.wrapped = wrapped;
		this.unpackedObjects = scanLoose();
	}

	private ObjectIdOwnerMap<UnpackedObjectId> scanLoose() {
		ObjectIdOwnerMap<UnpackedObjectId> m = new ObjectIdOwnerMap<>();
		File objects = wrapped.getDirectory();
		String[] fanout = objects.list();
		if (fanout == null)
			return m;
		for (String d : fanout) {
			if (d.length() != 2)
				continue;
			String[] entries = new File(objects, d).list();
			if (entries == null)
				continue;
			for (String e : entries) {
				if (e.length() != Constants.OBJECT_ID_STRING_LENGTH - 2)
					continue;
				try {
					ObjectId id = ObjectId.fromString(d + e);
					m.add(new UnpackedObjectId(id));
				} catch (IllegalArgumentException notAnObject) {
					// ignoring the file that does not represent loose object
				}
			}
		}
		return m;
	}

	/** {@inheritDoc} */
	@Override
	public void close() {
		// Don't close anything.
	}

	/** {@inheritDoc} */
	@Override
	public ObjectDatabase newCachedDatabase() {
		return this;
	}

	@Override
	File getDirectory() {
		return wrapped.getDirectory();
	}

	@Override
	File fileFor(AnyObjectId id) {
		return wrapped.fileFor(id);
	}

	@Override
	Config getConfig() {
		return wrapped.getConfig();
	}

	@Override
	FS getFS() {
		return wrapped.getFS();
	}

	@Override
	Set<ObjectId> getShallowCommits() throws IOException {
		return wrapped.getShallowCommits();
	}

	private HDFSCachedObjectDirectory[] myAlternates() {
		if (alts == null) {
			HDFSObjectDirectory.AlternateHandle[] src = wrapped.myAlternates();
			alts = new HDFSCachedObjectDirectory[src.length];
			for (int i = 0; i < alts.length; i++)
				alts[i] = src[i].db.newCachedFileObjectDatabase();
		}
		return alts;
	}

	private Set<AlternateHandle.Id> skipMe(Set<AlternateHandle.Id> skips) {
		Set<AlternateHandle.Id> withMe = new HashSet<>();
		if (skips != null) {
			withMe.addAll(skips);
		}
		withMe.add(getAlternateId());
		return withMe;
	}

	@Override
	void resolve(Set<ObjectId> matches, AbbreviatedObjectId id)
			throws IOException {
		wrapped.resolve(matches, id);
	}

	/** {@inheritDoc} */
	@Override
	public boolean has(AnyObjectId objectId) throws IOException {
		return has(objectId, null);
	}

	private boolean has(AnyObjectId objectId, Set<AlternateHandle.Id> skips)
			throws IOException {
		if (unpackedObjects.contains(objectId)) {
			return true;
		}
		if (wrapped.hasPackedObject(objectId)) {
			return true;
		}
		skips = skipMe(skips);
		for (HDFSCachedObjectDirectory alt : myAlternates()) {
			if (!skips.contains(alt.getAlternateId())) {
				if (alt.has(objectId, skips)) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	ObjectLoader openObject(WindowCursor curs, AnyObjectId objectId)
			throws IOException {
		return openObject(curs, objectId, null);
	}

	private ObjectLoader openObject(final WindowCursor curs,
			final AnyObjectId objectId, Set<AlternateHandle.Id> skips)
			throws IOException {
		ObjectLoader ldr = openLooseObject(curs, objectId);
		if (ldr != null) {
			return ldr;
		}
		ldr = wrapped.openPackedObject(curs, objectId);
		if (ldr != null) {
			return ldr;
		}
		skips = skipMe(skips);
		for (HDFSCachedObjectDirectory alt : myAlternates()) {
			if (!skips.contains(alt.getAlternateId())) {
				ldr = alt.openObject(curs, objectId, skips);
				if (ldr != null) {
					return ldr;
				}
			}
		}
		return null;
	}

	@Override
	long getObjectSize(WindowCursor curs, AnyObjectId objectId)
			throws IOException {
		// Object size is unlikely to be requested from contexts using
		// this type. Don't bother trying to accelerate the lookup.
		return wrapped.getObjectSize(curs, objectId);
	}

	@Override
	ObjectLoader openLooseObject(WindowCursor curs, AnyObjectId id)
			throws IOException {
		if (unpackedObjects.contains(id)) {
			ObjectLoader ldr = wrapped.openLooseObject(curs, id);
			if (ldr != null)
				return ldr;
			unpackedObjects = scanLoose();
		}
		return null;
	}

	@Override
	InsertLooseObjectResult insertUnpackedObject(File tmp, ObjectId objectId,
			boolean createDuplicate) throws IOException {
		InsertLooseObjectResult result = wrapped.insertUnpackedObject(tmp,
				objectId, createDuplicate);
		switch (result) {
		case INSERTED:
		case EXISTS_LOOSE:
			unpackedObjects.addIfAbsent(new UnpackedObjectId(objectId));
			break;

		case EXISTS_PACKED:
		case FAILURE:
			break;
		}
		return result;
	}

	@Override
	PackFile openPack(File pack) throws IOException {
		return wrapped.openPack(pack);
	}

	@Override
	void selectObjectRepresentation(PackWriter packer, ObjectToPack otp,
			WindowCursor curs) throws IOException {
		wrapped.selectObjectRepresentation(packer, otp, curs);
	}

	@Override
	Collection<PackFile> getPacks() {
		return wrapped.getPacks();
	}

	private static class UnpackedObjectId extends ObjectIdOwnerMap.Entry {
		UnpackedObjectId(AnyObjectId id) {
			super(id);
		}
	}

	private AlternateHandle.Id getAlternateId() {
		return wrapped.getAlternateId();
	}
}

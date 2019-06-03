package org.eclipse.jgit.internal.storage.file;

import java.io.IOException;

import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.lib.BaseRepositoryBuilder;
import org.eclipse.jgit.lib.Repository;

/**
 * @author hyj
 *
 */
public class HDFSRepositoryBuilder
		extends BaseRepositoryBuilder<HDFSRepositoryBuilder, Repository> {
	@Override
	public HDFSRepositoryBuilder setup()
			throws IllegalArgumentException, IOException {
		setupWorkTree();
		setupInternals();
		return self();
	}

	@Override
	protected void setupInternals() throws IOException {
		// CHANGED use HDFSFile
		if (getObjectDirectory() == null && getGitDir() != null)
			if (getGitDir() instanceof HDFSFile) {
				setObjectDirectory(((HDFSFile) getGitDir()).search("objects")); //$NON-NLS-1$
			} else {
				setObjectDirectory(safeFS().resolve(getGitDir(), "objects")); //$NON-NLS-1$
			}
	}

	@Override
	public Repository build() throws IOException {
		// CHANGED use HDFSFileRepository
		HDFSFileRepository repo = new HDFSFileRepository(setup());
		if (isMustExist() && !repo.getObjectDatabase().exists())
			throw new RepositoryNotFoundException(getGitDir());
		return repo;
	}
}

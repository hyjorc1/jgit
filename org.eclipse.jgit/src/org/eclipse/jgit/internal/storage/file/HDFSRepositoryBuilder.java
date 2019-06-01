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
		requireGitDirOrWorkTree();
		setupGitDir();
		setupWorkTree();
		setupInternals();
		return self();
	}

	@Override
	// CHANGED use HDFSFileRepository
	public Repository build() throws IOException {
		HDFSFileRepository repo = new HDFSFileRepository(setup());
		if (isMustExist() && !repo.getObjectDatabase().exists())
			throw new RepositoryNotFoundException(getGitDir());
		return repo;
	}
}

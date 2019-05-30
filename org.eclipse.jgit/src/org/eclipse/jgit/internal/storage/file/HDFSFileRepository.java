package org.eclipse.jgit.internal.storage.file;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;

import org.eclipse.jgit.attributes.AttributesNodeProvider;
import org.eclipse.jgit.boa.HDFSRepositoryBuilder;
import org.eclipse.jgit.internal.JGitText;
import org.eclipse.jgit.lib.BaseRepositoryBuilder;
import org.eclipse.jgit.lib.ConfigConstants;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.RefDatabase;
import org.eclipse.jgit.lib.ReflogReader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.StoredConfig;
import org.eclipse.jgit.storage.file.FileBasedConfig;
import org.eclipse.jgit.util.FS;
import org.eclipse.jgit.util.StringUtils;
import org.eclipse.jgit.util.SystemReader;

/**
 * @author hyj
 *
 */
public class HDFSFileRepository extends Repository {

	private final FileBasedConfig systemConfig;
	private final FileBasedConfig userConfig;
	private final FileBasedConfig repoConfig;

	private final HDFSObjectDirectory objectDatabase;

	/**
	 * @param options
	 * @throws IOException
	 */
	public HDFSFileRepository(final BaseRepositoryBuilder<HDFSRepositoryBuilder, Repository> options) throws IOException {
		super(options);

		if (StringUtils.isEmptyOrNull(SystemReader.getInstance().getenv(
				Constants.GIT_CONFIG_NOSYSTEM_KEY)))
			systemConfig = SystemReader.getInstance().openSystemConfig(null,
					getFS());
		else
			systemConfig = new FileBasedConfig(null, FS.DETECTED) {
				@Override
				public void load() {
					// empty, do not load
				}

				@Override
				public boolean isOutdated() {
					// regular class would bomb here
					return false;
				}
			};
		userConfig = SystemReader.getInstance().openUserConfig(systemConfig,
				getFS());
		repoConfig = new FileBasedConfig(userConfig, getFS().resolve(
				getDirectory(), Constants.CONFIG),
				getFS());

		repoConfig.addChangeListener(this::fireEvent);

		final long repositoryFormatVersion = getConfig().getLong(
				ConfigConstants.CONFIG_CORE_SECTION, null,
				ConfigConstants.CONFIG_KEY_REPO_FORMAT_VERSION, 0);

		objectDatabase = new HDFSObjectDirectory(repoConfig, //
				options.getObjectDirectory(), //
				options.getAlternateObjectDirectories(), //
				getFS(), //
				new File(getDirectory(), Constants.SHALLOW));

		if (objectDatabase.exists()) {
			if (repositoryFormatVersion > 1)
				throw new IOException(MessageFormat.format(
						JGitText.get().unknownRepositoryFormat2,
						Long.valueOf(repositoryFormatVersion)));
		}
	}

	@Override
	public void create(boolean bare) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public HDFSObjectDirectory getObjectDatabase() {
		return objectDatabase;
	}

	@Override
	public RefDatabase getRefDatabase() {
		return (RefDatabase) new Object();
	}

	@Override
	public StoredConfig getConfig() {
		return repoConfig;
	}

	@Override
	public void scanForRepoChanges() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public ReflogReader getReflogReader(String refName) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AttributesNodeProvider createAttributesNodeProvider() {
		// TODO Auto-generated method stub
		return (AttributesNodeProvider) new Object();
	}

	@Override
	public void notifyIndexChanged(boolean internal) {
		// TODO Auto-generated method stub
	}


}

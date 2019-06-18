package org.eclipse.jgit.internal.storage.file;

import java.io.IOException;
import org.eclipse.jgit.attributes.AttributesNodeProvider;
//import org.eclipse.jgit.internal.JGitText;
import org.eclipse.jgit.lib.BaseRepositoryBuilder;
//import org.eclipse.jgit.lib.ConfigConstants;
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
public class ByteArrayFileRepository extends Repository {

	private final FileBasedConfig systemConfig;
	private final FileBasedConfig userConfig;
	private final FileBasedConfig repoConfig;

	private final ByteArrayObjectDirectory objectDatabase;

	/**
	 * @param options
	 * @throws IOException
	 */

	public ByteArrayFileRepository(final BaseRepositoryBuilder<ByteArrayRepositoryBuilder, Repository> options) throws IOException {
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

		// CHANGED use HDFSObjectDirectory
		objectDatabase = new ByteArrayObjectDirectory(repoConfig,
				(ByteArrayFile) options.getObjectDirectory(),
				getFS(),
				new ByteArrayFile(getDirectory(), Constants.SHALLOW));
	}

	/**
	 * Invoked when the use count drops to zero during {@link #close()}.
	 * <p>
	 * The default implementation closes the object and ref databases.
	 */
	@Override
	protected void doClose() {
		getObjectDatabase().close();
	}

	@Override
	public void create(boolean bare) throws IOException {
		// IGNOREME
	}

	@Override
	public ByteArrayObjectDirectory getObjectDatabase() {
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
		// IGNOREME
	}

	@Override
	public ReflogReader getReflogReader(String refName) throws IOException {
		return null;
	}

	@Override
	public AttributesNodeProvider createAttributesNodeProvider() {
		return (AttributesNodeProvider) new Object();
	}

	@Override
	public void notifyIndexChanged(boolean internal) {
		// IGNOREME
	}

}

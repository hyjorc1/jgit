package org.eclipse.jgit.internal.storage.boa;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.eclipse.jgit.util.FS;

/**
 * @author hyj
 *
 */
public class HDFSFile extends java.io.File {

	private static final long serialVersionUID = 1L;

	private boolean isDirectory;
	private String path;
	private HDFSFile parent;
	private int idx;
	private List<HDFSFile> list;
	private byte[] bytes;

	// for HDFSFileSnapshot
	private long modified;
	private long size;

	/**
	 * @param path
	 */
	public HDFSFile(String path) {
		super(path);
		this.path = path;
		this.parent = null;
		this.idx = -1;
		this.list = null;
		this.bytes = null;
		this.isDirectory = false;
		this.modified = -1;
		this.size = -1;
	}

	private HDFSFile(String name, HDFSFile parent, int idx) {
		this(name);
		this.parent = parent;
		this.idx = idx;
	}

	/* -- build NodeFile -- */

	/**
	 * @return HDFSFile
	 */
	public HDFSFile build() {
		File file = new File(this.path);
		Stack<File> files = new Stack<>();
		Stack<HDFSFile> nodes = new Stack<>();
		nodes.push(this);
		files.push(file);
		while (!files.isEmpty()) {
			HDFSFile curNode = nodes.pop();
			File curFile = files.pop();
			if (curFile.isFile()) {
				curNode.updateModifiedAndSize(curFile);
				curNode.isDirectory = false;
				curNode.bytes = getBytes(curFile);
			} else {
				curNode.updateModifiedAndSize(curFile);
				curNode.isDirectory = true;
				curNode.list = new ArrayList<>();
				for (File f : curFile.listFiles()) {
					HDFSFile childNode = new HDFSFile(f.getName(), curNode,
							list.size());
					curNode.list.add(childNode);
					nodes.push(childNode);
					files.push(f);
				}
			}
		}
		return this;
	}

	private void updateModifiedAndSize(File curFile) {
		BasicFileAttributes fileAttributes;
		try {
			fileAttributes = FS.DETECTED.fileAttributes(curFile);
			modified = fileAttributes.lastModifiedTime().toMillis();
			size = fileAttributes.size();
		} catch (IOException e) {
			modified = curFile.lastModified();
			size = curFile.length();
		}
	}

	private byte[] getBytes(File file) {
		byte[] bytes = new byte[(int) file.length()];

		FileInputStream fileInputStream = null;
		try {
			fileInputStream = new FileInputStream(file);
			fileInputStream.read(bytes);
			fileInputStream.close();
			return bytes;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	/* -- write NodeFile -- */

	/**
	 * @param outputPath
	 * @throws IOException
	 */
	public void writeContentsToDir(String outputPath) throws IOException {
		if (!outputPath.endsWith(File.separator))
			outputPath += File.separator;
		writeToFile(outputPath, this);
		System.out.println("Done writing to " + outputPath);
	}

	private void writeToFile(String parentPath, HDFSFile node)
			throws IOException {
		Stack<String> parentPaths = new Stack<>();
		Stack<HDFSFile> nodes = new Stack<>();
		nodes.push(node);
		parentPaths.push(parentPath);
		while (!nodes.isEmpty()) {
			HDFSFile curNode = nodes.pop();
			String curParentPath = parentPaths.pop();
			if (curNode.isDirectory) {
				curParentPath += File.separator + curNode.getName();
				new File(curParentPath).mkdirs();
				for (HDFSFile childNode : curNode.list) {
					nodes.push(childNode);
					parentPaths.push(curParentPath);
				}
			} else {
				File file = new File(curParentPath, curNode.getName());
				file.setReadable(true, false);
				file.setWritable(true, false);

				FileOutputStream fos = null;
				try {
					fos = new FileOutputStream(file);
					fos.write(curNode.bytes);
					fos.flush();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					if (fos != null)
						fos.close();
				}
			}
		}
	}

	/**
	 * @return list
	 */
	public List<HDFSFile> getList() {
		return list;
	}

	/**
	 * @return bytes
	 */
	public byte[] getBytes() {
		return bytes;
	}

	/**
	 * @return idx
	 */
	public int getIdx() {
		return idx;
	}

	/**
	 * @param modified
	 */
	public void setModified(long modified) {
		this.modified = modified;
	}

	/**
	 * @param size
	 */
	public void setSize(long size) {
		this.size = size;
	}

	/* -- Path-component accessors -- */

	@Override
	public String getName() {
		int index = path.lastIndexOf(separatorChar);
		if (index != -1)
			return path.substring(index + 1);
		return path;
	}

	@Override
	public String getParent() {
		return parent.getPath();
	}

	@Override
	public HDFSFile getParentFile() {
		return parent;
	}

	@Override
	public String getPath() {
		return path;
	}

	@Override
	public boolean isAbsolute() {
		return true;
	}

	@Override
	public String getAbsolutePath() {
		return path;
	}

	@Override
	public HDFSFile getAbsoluteFile() {
		return this;
	}

	@Override
	public String getCanonicalPath() {
		return path;
	}

	@Override
	public HDFSFile getCanonicalFile() {
		return this;
	}

	/* -- Attribute accessors -- */

	@Override
	public boolean canRead() {
		return true;
	}

	@Override
	public boolean canWrite() {
		return false;
	}

	@Override
	public boolean exists() {
		return true;
	}

	@Override
	public boolean isDirectory() {
		return isDirectory;
	}

	@Override
	public boolean isFile() {
		return !isDirectory;
	}

	@Override
	public boolean isHidden() {
		return false;
	}

	@Override
	public long lastModified() {
		return modified;
	}

	@Override
	public long length() {
		return size;
	}

	/* -- File operations -- */

	@Override
	public boolean createNewFile() throws IOException {
		return false;
	}

	@Override
	public boolean delete() {
		if (parent != null && idx != -1) {
			parent.getList().remove(idx);
			return true;
		}
		return false;
	}

	@Override
	public void deleteOnExit() {

	}

	@Override
	public String[] list() {
		if (list != null) {
			String[] names = new String[list.size()];
			for (int i = 0; i < names.length; i++)
				names[i] = list.get(i).getName();
			return names;
		}
		return null;
	}

	@Override
	public String[] list(FilenameFilter filter) {
		String names[] = list();
		if ((names == null) || (filter == null)) {
			return names;
		}
		List<String> v = new ArrayList<>();
		for (int i = 0; i < names.length; i++) {
			if (filter.accept(this, names[i])) {
				v.add(names[i]);
			}
		}
		return v.toArray(new String[v.size()]);
	}

	@Override
	public HDFSFile[] listFiles() {
		if (list != null)
			return list.toArray(new HDFSFile[list.size()]);
		return null;
	}

	@Override
	public HDFSFile[] listFiles(FilenameFilter filter) {
		if (list == null)
			return null;
		ArrayList<HDFSFile> files = new ArrayList<>();
		for (HDFSFile n : list)
			if ((filter == null) || filter.accept(this, n.getName()))
				files.add(n);
		return files.toArray(new HDFSFile[files.size()]);
	}

	@Override
	public HDFSFile[] listFiles(FileFilter filter) {
		if (list == null)
			return null;
		ArrayList<HDFSFile> files = new ArrayList<>();
		for (HDFSFile n : list)
			if ((filter == null) || filter.accept(n))
				files.add(n);
		return files.toArray(new HDFSFile[files.size()]);
	}

	@Override
	public boolean mkdir() {
		return true;
	}

	@Override
	public boolean mkdirs() {
		return true;
	}

	@Override
	public boolean renameTo(File dest) {
		return true;
	}

	@Override
	public boolean setLastModified(long time) {
		return true;
	}

	@Override
	public boolean setReadOnly() {
		return true;
	}

	@Override
	public boolean setWritable(boolean writable, boolean ownerOnly) {
		return true;
	}

	@Override
	public boolean setWritable(boolean writable) {
		return true;
	}

	@Override
	public boolean setReadable(boolean readable, boolean ownerOnly) {
		return true;
	}

	@Override
	public boolean setReadable(boolean readable) {
		return true;
	}

	@Override
	public boolean setExecutable(boolean executable, boolean ownerOnly) {
		return true;
	}

	@Override
	public boolean setExecutable(boolean executable) {
		return true;
	}

	@Override
	public boolean canExecute() {
		return false;
	}

	/* -- Filesystem interface -- */
	/* -- Disk usage -- */
	/* -- Temporary files -- */
	/* -- Basic infrastructure -- */
	@Override
	public int compareTo(File file) {
		return path.compareTo(file.getPath());
	}

	@Override
	public boolean equals(Object obj) {
		if ((obj != null) && (obj instanceof HDFSFile))
			return compareTo((HDFSFile) obj) == 0;
		return false;
	}

	@Override
	public int hashCode() {
		return this.getName().hashCode();
	}

	@Override
	public String toString() {
		return getPath();
	}

	// @Override
	// private synchronized void writeObject(java.io.ObjectOutputStream s)
	// throws IOException {
	// s.defaultWriteObject();
	// s.writeChar(separatorChar); // Add the separator character
	// }

}

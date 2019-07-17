package org.eclipse.jgit.internal.storage.file;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import org.eclipse.jgit.util.FS;

/**
 * @author hyj
 *
 */
public class ByteArrayFile extends java.io.File {

	private static final long serialVersionUID = 1L;

	private boolean isDirectory;
	private boolean isBuilt;

	private String path;
	private ByteArrayFile parent;
	private int idx;
	private List<ByteArrayFile> list;
	private byte[] bytes;

	// for HDFSFileSnapshot
	private long modified;
	private long size;

	/* -- HDFSFile Constructor -- */

	/**
	 * @param path
	 */
	public ByteArrayFile(String path) {
		super(path);
		set(false, false, path, null, -1, null, null, -1, -1);
		if (build())
			isBuilt = true;
	}

	private ByteArrayFile(String path, ByteArrayFile parent, int idx) {
		super(path);
		set(false, false, path, parent, idx, null, null, -1, -1);
	}

	/**
	 * @param parent
	 * @param child
	 */
	public ByteArrayFile(File parent, String child) {
		super(parent, child);
		if (parent instanceof ByteArrayFile) {
			ByteArrayFile f = ((ByteArrayFile) parent).findChild(child);
			if (f != null)
				set(f.isDirectory, f.isBuilt, f.path,
						f.parent, f.idx,
						f.list,
						f.bytes,
						f.modified, f.size);
		}
	}

	private void set(boolean isDirectory, boolean isBuilt, String path,
			ByteArrayFile parent, int idx,
			List<ByteArrayFile> list, byte[] bytes, long modified, long size) {
		this.isDirectory = isDirectory;
		this.isBuilt = isBuilt;
		this.path = path;
		this.parent = parent;
		this.idx = idx;
		this.list = list;
		this.bytes = bytes;
		this.modified = modified;
		this.size = size;
	}

	private ByteArrayFile findChild(String child) {
		ByteArrayFile file = null;
		if (isDirectory)
			for (ByteArrayFile f : list)
				if (f.getName().equals(child))
					file = f;
		return file;
	}

	/**
	 * @param name
	 * @return cur or null
	 */
	public ByteArrayFile search(String name) {
		LinkedList<ByteArrayFile> q = new LinkedList<>();
		q.offer(this);
		while (!q.isEmpty()) {
			ByteArrayFile cur = q.poll();
			if (cur.getName().equals(name))
				return cur;
			if (cur.isDirectory)
				for (ByteArrayFile f : list)
					q.offer(f);
		}
		return null;
	}

	/* -- build HDFSFile -- */

	/**
	 * @return HDFSFile
	 */
	private boolean build() {
		File file = new File(this.path);
		Stack<File> files = new Stack<>();
		Stack<ByteArrayFile> nodes = new Stack<>();
		nodes.push(this);
		files.push(file);
		while (!files.isEmpty()) {
			ByteArrayFile curNode = nodes.pop();
			File curFile = files.pop();
			if (curFile.isFile()) {
				curNode.updateModifiedAndSize(curFile);
				curNode.isDirectory = false;
				curNode.bytes = getBytes(curFile);
				if (curNode.bytes == null)
					return false;
			} else {
				curNode.updateModifiedAndSize(curFile);
				curNode.isDirectory = true;
				curNode.list = new ArrayList<>();
				for (File f : curFile.listFiles()) {
					ByteArrayFile childNode = new ByteArrayFile(f.getPath(), curNode,
							list.size());
					curNode.list.add(childNode);
					nodes.push(childNode);
					files.push(f);
				}
			}
		}
		return true;
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

	/**
	 * @param file
	 * @return null
	 */
	@SuppressWarnings("resource")
	public byte[] getBytes(File file) {
		if (file.length() <= Integer.MAX_VALUE / 3) {
			byte[] ba = new byte[(int) file.length()];
			FileInputStream fileInputStream = null;
			try {
				fileInputStream = new FileInputStream(file);
				fileInputStream.read(ba);
				fileInputStream.close();
				return ba;
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
		System.out.println("Done writing to " + outputPath); //$NON-NLS-1$
	}

	@SuppressWarnings("resource")
	private void writeToFile(String parentPath, ByteArrayFile node)
			throws IOException {
		Stack<String> parentPaths = new Stack<>();
		Stack<ByteArrayFile> nodes = new Stack<>();
		nodes.push(node);
		parentPaths.push(parentPath);
		while (!nodes.isEmpty()) {
			ByteArrayFile curNode = nodes.pop();
			String curParentPath = parentPaths.pop();
			if (curNode.isDirectory) {
				curParentPath += File.separator + curNode.getName();
				new File(curParentPath).mkdirs();
				for (ByteArrayFile childNode : curNode.list) {
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
	 * @return isBuilt
	 */
	public boolean isBuilt() {
		return isBuilt;
	}

	/**
	 * @param isBuilt
	 */
	public void setBuilt(boolean isBuilt) {
		this.isBuilt = isBuilt;
	}

	/**
	 * @return list
	 */
	public List<ByteArrayFile> getList() {
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
	public ByteArrayFile getParentFile() {
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
	public ByteArrayFile getAbsoluteFile() {
		return this;
	}

	@Override
	public String getCanonicalPath() {
		return path;
	}

	@Override
	public ByteArrayFile getCanonicalFile() {
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
		// IGNOREME
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
	public ByteArrayFile[] listFiles() {
		if (list != null)
			return list.toArray(new ByteArrayFile[list.size()]);
		return null;
	}

	@Override
	public ByteArrayFile[] listFiles(FilenameFilter filter) {
		if (list == null)
			return null;
		ArrayList<ByteArrayFile> files = new ArrayList<>();
		for (ByteArrayFile n : list)
			if ((filter == null) || filter.accept(this, n.getName()))
				files.add(n);
		return files.toArray(new ByteArrayFile[files.size()]);
	}

	@Override
	public ByteArrayFile[] listFiles(FileFilter filter) {
		if (list == null)
			return null;
		ArrayList<ByteArrayFile> files = new ArrayList<>();
		for (ByteArrayFile n : list)
			if ((filter == null) || filter.accept(n))
				files.add(n);
		return files.toArray(new ByteArrayFile[files.size()]);
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
		if ((obj != null) && (obj instanceof ByteArrayFile))
			return compareTo((ByteArrayFile) obj) == 0;
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
}

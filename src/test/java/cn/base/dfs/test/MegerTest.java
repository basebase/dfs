package cn.base.dfs.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class MegerTest {
	
	public static void main(String[] args) throws IOException {
		new MegerTest().mergePartFiles(null, null, 500, "test");
	}

	public void mergePartFiles(String dirPath, String partFileSuffix, int partFileSize, String mergeFileName)
			throws IOException {

		File file1 = new File("/Users/Joker/Desktop/dfs/datanode/b/b_1");
		File file2 = new File("/Users/Joker/Desktop/dfs/datanode/b/b_2");
		File file3 = new File("/Users/Joker/Desktop/dfs/datanode/b/b_3");

		ArrayList<File> files = new ArrayList<File>();
		files.add(file1);
		files.add(file2);
		files.add(file3);

		Collections.sort(files, new FileComparator());
		RandomAccessFile randomAccessFile = new RandomAccessFile(mergeFileName, "rw");
		randomAccessFile.setLength(partFileSize * (files.size() - 1) + files.get(files.size() - 1).length());
		randomAccessFile.close();
		
		long start = 0;
		long end = 0;
		for (int i = 0; i < files.size(); i++) {
			
			start = end;
			end = (i + 1) * partFileSize;
			MegerRunnable megerRunnable = new MegerRunnable(start, mergeFileName, files.get(i));
			new Thread(megerRunnable).start();
			
			
		}
	}

	private class FileComparator implements Comparator<File> {
		@Override
		public int compare(File o1, File o2) {
			return o1.getName().compareToIgnoreCase(o2.getName());
		}
	}

}

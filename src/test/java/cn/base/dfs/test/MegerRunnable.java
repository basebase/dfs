package cn.base.dfs.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;

public class MegerRunnable implements Runnable {

	private long startPos;
	private String mergeFileName;
	private File partFile;
	
	public MegerRunnable(long startPos, String mergeFileName, File partFile) {
		this.startPos = startPos;
		this.mergeFileName = mergeFileName;
		this.partFile = partFile;
	}
	
	
	@Override
	public void run() {
		RandomAccessFile rFile = null;
		
		try {
			
			rFile = new RandomAccessFile(mergeFileName, "rw");
			rFile.seek(startPos);
			FileInputStream fs = new FileInputStream(partFile);
			int available = fs.available();
			byte[] b = new byte[available];
			System.out.println("b length -> " + b.length + "   available = " + available);
			fs.read(b);
			fs.close();
			rFile.write(b);
			rFile.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

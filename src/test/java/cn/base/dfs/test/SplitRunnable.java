package cn.base.dfs.test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;

public class SplitRunnable implements Runnable {

	private static Object lock = new Object();
	
	private int start;
	private int end;
	private int byteSize;
	private String partFileName;
	private File originFile;
	private int startPos;

	public SplitRunnable(int byteSize, int startPos, String partFileName, File originFile) {
		this.startPos = startPos;
		this.byteSize = byteSize;
		this.partFileName = partFileName;
		this.originFile = originFile;
	}

	@Override
	public void run() {
		synchronized (lock) {
		
			RandomAccessFile rFile;
			OutputStream os;
			BufferedInputStream in = null;
			
			
			try {

				rFile = new RandomAccessFile(originFile, "r");
				in = new BufferedInputStream(new FileInputStream(originFile));
				
				
				byte[] b = new byte[byteSize];
				rFile.seek(startPos);
				int s = rFile.read(b);
				
				
				os = new FileOutputStream(partFileName);
				
				
				os.write(b, 0, s);
				os.flush();
				os.close();

				System.out.println(Thread.currentThread().getName() + " end ...");

			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
	}

}

package cn.base.dfs.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class SeekTest {
	
	public static void main(String[] args) throws IOException {
		RandomAccessFile accessFile = new RandomAccessFile("/Users/Joker/Desktop/a", "r");
		accessFile.seek(1000);
		byte[] bs = new byte[1000];
		int s = accessFile.read(bs);
		String string = new String(bs, 0, s);
		System.out.println(string);
	}
}

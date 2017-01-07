package cn.base.dfs.test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.thoughtworks.xstream.mapper.PackageAliasingMapper;

public class SplitFileTest {

	public List<String> splitBySize(String fileName, int byteSize) {
		List<String> parts = new ArrayList<String>();
		File file = new File(fileName);
		int count = (int) Math.ceil(file.length() / (double) byteSize);
		int countLen = (count + "").length();

		int remainder = (int) (file.length() % byteSize);

		int start = 0;
		int end = 0;
		for (int i = 0; i < count; i++) {
			
			if (i == count - 1) {

				start = end;
				end = start + remainder;
				
				System.out.println("start = " + start + " end = " + end);
				
				String partFileName = file.getName() + "." + (i + 1) + "." + "part";
				SplitRunnable runnable = new SplitRunnable(remainder, start, partFileName, file);

				new Thread(runnable).start();
				parts.add(partFileName);

			} else if (i == 0) {
				
				start = 0;
				end = byteSize;
				
				System.out.println("start = " + start + " end = " + end);
				
				String partFileName = file.getName() + "." + (i + 1) + "." + "part";
				SplitRunnable runnable = new SplitRunnable(byteSize, start, partFileName, file);
				new Thread(runnable).start();
				parts.add(partFileName);
			} else {
				
				start = end;
				end = start * byteSize;
				
				System.out.println("start = " + start + " end = " + end);
				
				String partFileName = file.getName() + "." + (i + 1) + "." + "part";
				SplitRunnable runnable = new SplitRunnable(byteSize, start * byteSize, partFileName, file);
				new Thread(runnable).start();
				parts.add(partFileName);
			}
		}

		return parts;
	}

	public static void main(String[] args) {
		new SplitFileTest().splitBySize("/Users/Joker/Desktop/a", 251658240);
		System.out.println("main end...");
	}

}

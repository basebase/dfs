package cn.base.dfs.util;

import java.io.File;

import org.apache.log4j.Logger;

/***
 * 获取磁盘空间大小
 * 
 * @author Joker
 *
 */
public class DiskInfoUtil {

	private static final Logger LOGGER = Logger.getLogger(DiskInfoUtil.class);

	public static boolean getDiskSize(String path, String fileName) {
		if (path == null || !new File(path).exists()) {
			return false;
		}
		
		// 文件大小(MB)
		long size = new File(fileName).length() / 1024 / 1024 / 1024;
		
		File file = new File(path);
		long total = file.getFreeSpace() / 1024 / 1024;
		
		if (size > total) {
			LOGGER.info(fileName + " Beyond the size of disk !");
			return false;
		}
		
		LOGGER.info(file.getFreeSpace() / 1024 / 1024 + " MB" + " Don't use" );
		return true;
	}
}

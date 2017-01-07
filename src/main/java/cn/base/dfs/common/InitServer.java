package cn.base.dfs.common;

import java.io.File;

import org.apache.log4j.Logger;

import cn.base.dfs.configuration.ProperConf;

public class InitServer {

	private static final Logger LOGGER = Logger.getLogger(InitServer.class);

	public static boolean initServer(String nodeDir) {
		File file = new File(nodeDir);
		if (!file.exists()) {
			if (file.mkdirs()) {
				LOGGER.info("create " + nodeDir + "  Directory success !");
				return true;
			} else {
				LOGGER.info("Failed to create Durectory ");
				return false;
			}
		} else {
			LOGGER.info(nodeDir + " The directory already exists");
			return true;
		}
	}
}

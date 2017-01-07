package cn.base.dfs.configuration;

import java.io.IOException;
import java.util.Properties;

import cn.base.dfs.Start;

public class ProperConf {

	public static Properties configuration = new Properties();

	/***
	 * 启动的时候加载配置文件
	 */
	static {
		try {
			configuration.load(ProperConf.class.getClassLoader().getResourceAsStream("dfs-site.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

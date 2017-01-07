package cn.base.dfs.common;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.log4j.Logger;

import cn.base.dfs.configuration.ServerConfiguration;

public class MetaServerCheckStart {

	private static final Logger LOGGER = Logger.getLogger(MetaServerCheckStart.class);

	/***
	 * 元数据服务是否启动
	 * 
	 * @return
	 */
	public static boolean metaServerIsStart() {

		String url = "http://" + ServerConfiguration.HOSTNAME + ":" + ServerConfiguration.META_DEFAULT_PORT;
		try {
			final URLConnection readConnection = new URL(url).openConnection();
			readConnection.connect();
			LOGGER.info("Service " + url + " available, yeah!");
			return true;
		} catch (final MalformedURLException e) {
			throw new IllegalStateException("Bad URL: " + url, e);
		} catch (final IOException e) {
			LOGGER.info("Service " + url + " unavailable, oh no!", e);
			return false;
		}
	}
	
	/**
	 * 节点是否启动
	 * @return
	 */
	public static boolean nodeServerIsStart() {

		String url = "http://" + ServerConfiguration.HOSTNAME + ":" + ServerConfiguration.NODE_PORT;
		try {
			final URLConnection readConnection = new URL(url).openConnection();
			readConnection.connect();
			LOGGER.info("Service " + url + " available, yeah!");
			return true;
		} catch (final MalformedURLException e) {
			throw new IllegalStateException("Bad URL: " + url, e);
		} catch (final IOException e) {
			LOGGER.info("Service " + url + " unavailable, oh no!", e);
			return false;
		}
	}

}

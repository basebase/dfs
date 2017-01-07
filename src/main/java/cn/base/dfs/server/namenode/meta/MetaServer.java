package cn.base.dfs.server.namenode.meta;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import org.apache.log4j.Logger;
import org.apache.mina.api.AbstractIoHandler;
import org.apache.mina.api.IoSession;
import org.apache.mina.codec.IoBuffer;
import org.apache.mina.codec.delimited.serialization.JavaNativeMessageDecoder;
import org.apache.mina.codec.delimited.serialization.JavaNativeMessageEncoder;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.nio.NioTcpServer;

import cn.base.dfs.configuration.ProperConf;
import cn.base.dfs.configuration.ServerConfiguration;

public class MetaServer {

	private static final Logger LOG = Logger.getLogger(MetaServer.class);
	private static HashMap<String, ArrayList<String>> metaMap = new HashMap<String, ArrayList<String>>();

	public static void main(String[] args) {
		MetaServer metaServer = new MetaServer();
		metaServer.startServer();
	}
	
	/***
	 * 启动服务
	 */
	public void startServer() {
		LOG.info("start readMeta server...");
		initMeta(); // 初始化元数据信息
		final NioTcpServer acceptor = new NioTcpServer();
		acceptor.setFilters(new LoggingFilter("LoggingFilter1"));
		acceptor.setIoHandler(new AbstractIoHandler() {
			@Override
			public void sessionOpened(IoSession session) {
				LOG.info("welcome client " + session.getLocalAddress() + " connection");
			}

			@Override
			public void sessionClosed(IoSession session) {
				LOG.info("client connection closed !");
			}

			@Override
			public void messageReceived(IoSession session, Object message) {
				if (message instanceof ByteBuffer) {

					ByteBuffer bf = (ByteBuffer) message;
					IoBuffer ioBuffer = IoBuffer.wrap(bf);

					// 如果是写入一律都是map形式，如果是删除，查询等一律都是str
					JavaNativeMessageDecoder<HashMap<String, ArrayList<String>>> mapDecoder = new JavaNativeMessageDecoder<HashMap<String, ArrayList<String>>>();
					JavaNativeMessageDecoder<ArrayList<String>> listDecoder = new JavaNativeMessageDecoder<ArrayList<String>>();

					HashMap<String, ArrayList<String>> blockMap = null; // mapDecoder.decode(ioBuffer);
					ArrayList<String> key = null;

					try {

						blockMap = mapDecoder.decode(ioBuffer);
						if (blockMap != null) {
							Set<String> vKey = blockMap.keySet();
							String originFileName = vKey.iterator().next();
							ArrayList<String> blocks = blockMap.get(originFileName);
							boolean storage = storageMeta(originFileName, blocks);

							JavaNativeMessageEncoder<Boolean> jme = new JavaNativeMessageEncoder<Boolean>();
							ByteBuffer result = jme.encode(storage);
							session.write(result);
							if (storage) {
								LOG.info("write meta data success !");
							} else {
								LOG.info("write meta data faild May be network reason sleep metaServer restart or 10 min synchronous !");
							}
						}
					} catch (ClassCastException e) {
						try {

							ioBuffer.clear();
							key = listDecoder.decode(ioBuffer);

							if (key != null) {
								// 查询元数据信息
								if (key.size() != 2) {
									LOG.info("Please check the sent over list length");
									return;
								}

								String action = key.get(0);
								if (action.equals("read")) {
									String readMeta = readMeta(key.get(1));
									LOG.info("Meta Data => " + readMeta);

									JavaNativeMessageEncoder<String> encoder = new JavaNativeMessageEncoder<String>();
									
									if (readMeta != null) {
										ByteBuffer blockBuff = encoder.encode(readMeta);
										session.write(blockBuff);
										LOG.info("There is this file");
									} else {
										ByteBuffer blockBuff = encoder.encode("NF"); // not Found 
										session.write(blockBuff);
										LOG.info("There is no this file");
									}
								}
							}

						} catch (ClassCastException e2) {
							e2.printStackTrace();
							return;
						}
					}
				}
			}

			@Override
			public void messageSent(IoSession session, Object message) {
				LOG.info("readMeta server send message: " + message);
			}
		});

		try {

			final SocketAddress address = new InetSocketAddress(ServerConfiguration.META_DEFAULT_PORT);
			acceptor.bind(address);
			Thread.sleep(1);
			new BufferedReader(new InputStreamReader(System.in)).readLine();
			acceptor.unbind();
		} catch (final InterruptedException e) {
			LOG.error("Interrupted exception", e);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/***
	 * 读取元数据信息
	 * 
	 * @param originFileName
	 * @return
	 */
	public String readMeta(String originFileName) {

		if (originFileName != null && !originFileName.equals("")) {
			ArrayList<String> blocks = metaMap.get(originFileName);

			if (blocks == null)
				return null;
			StringBuffer buff = new StringBuffer();
			for (String block : blocks) {
				buff.append(block).append(",");
			}

			buff.deleteCharAt(buff.length() - 1);
			return buff.toString();
		}

		return null;
	}

	/***
	 * 元数据写入内存
	 * 
	 * @param originFileName
	 * @param metas
	 * @return
	 */
	public boolean storageMeta(String originFileName, ArrayList<String> metas) {

		try {

			metaMap.put(originFileName, metas);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean check(String str) {
		if (str == null || str.equals("")) {
			return false;
		}
		return true;
	}

	/****
	 * 启动并读取元数据
	 */
	public void initMeta() {
		try {

			String dataNodeDir = (String) ProperConf.configuration.get("dfs.datanode.data.dir");

			if (!check(dataNodeDir)) {
				LOG.info("please Initialize the DataNode");
				return;
			}

			File[] listFiles = new File(dataNodeDir).listFiles();
			if (listFiles != null) {
				for (File f : listFiles) {
					if (f.isDirectory()) {
						ArrayList<String> files = new ArrayList<String>();
						File[] blockFiles = f.listFiles();
						for (File blockFile : blockFiles) {
							if (!blockFile.getName().endsWith(".meta")) {
								files.add(blockFile.getPath());
							}
						}
						metaMap.put(f.getName(), files);
					}
				}
			}

			LOG.info("Metadata reads");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

package cn.base.dfs.server.namenode;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.mina.api.AbstractIoHandler;
import org.apache.mina.api.IoSession;
import org.apache.mina.codec.IoBuffer;
import org.apache.mina.codec.delimited.serialization.JavaNativeMessageDecoder;
import org.apache.mina.codec.delimited.serialization.JavaNativeMessageEncoder;
import org.apache.mina.transport.nio.NioTcpClient;

import cn.base.dfs.Start;
import cn.base.dfs.common.InitServer;
import cn.base.dfs.common.MetaServerCheckStart;
import cn.base.dfs.configuration.ProperConf;
import cn.base.dfs.configuration.ServerConfiguration;
import cn.base.dfs.server.datanode.DataNode;
import cn.base.dfs.server.namenode.meta.MetaServerConnection;
import cn.base.dfs.util.DiskInfoUtil;

public class NameNode implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = Logger.getLogger(NameNode.class);

	private static final String DATANODEDIR = (String) ProperConf.configuration.get("dfs.datanode.data.dir");
	private static final String NAMENODEDIR = (String) ProperConf.configuration.get("dfs.namenode.name.dir");

	private boolean isInit = false;
	public static Map<String, List<String>> meta = new ConcurrentHashMap<String, List<String>>();

	public boolean check(String str) {
		if (str == null || str.equals("")) {
			return false;
		}
		return true;
	}

	public boolean checkSuccessFile(String baseDir) {
		File file = new File(baseDir, Start.SUCCESS_FILE_NAME);
		if (file.exists()) {
			return true;
		}
		return false;
	}

	public boolean checkDataNodeIsStart() {
		if (!checkSuccessFile(DATANODEDIR)) {
			LOGGER.info("Please start the first DataNode !");
			return false;
		}

		return true;
	}

	public boolean checkNameNodeIsStart() {
		if (!checkSuccessFile(NAMENODEDIR)) {
			LOGGER.info("Please start the first NameNode !");
			return false;
		}

		return true;
	}

	// 用来接收元数据返回的block信息
	String blocks = null;
	// 接收到响应信息的标记
	int count = 0;

	/***
	 * 通过查询元数据信息，在写入之前判断文件是否已经存在了
	 * 
	 * @param fileName
	 * @return
	 * @throws InterruptedException
	 */
	public boolean findFile(String fileName) throws InterruptedException {

		if (!MetaServerCheckStart.metaServerIsStart()) {
			LOGGER.info("please start meta server...");
			return false;
		}

		NioTcpClient client = new NioTcpClient();
		client.setIoHandler(new AbstractIoHandler() {

			@Override
			public void sessionOpened(IoSession session) {
				LOGGER.info("client conncetion ");
			}

			@Override
			public void sessionClosed(IoSession session) {
				LOGGER.info("client closed ");
			}

			@Override
			public void messageReceived(IoSession session, Object message) {
				if (message instanceof ByteBuffer) {
					ByteBuffer bf = (ByteBuffer) message;
					IoBuffer ioBuffer = IoBuffer.wrap(bf);
					JavaNativeMessageDecoder<String> decoder = new JavaNativeMessageDecoder<String>();
					String data = decoder.decode(ioBuffer);
					if (data != null && !data.equals("")) {
						blocks = data;
						++count;
						return;
					}

					++count;
				}
			}

			@Override
			public void messageSent(IoSession session, Object message) {
				LOGGER.info("sent message => " + message);
			}
		});

		IoSession session = MetaServerConnection.getSession(client, ServerConfiguration.HOSTNAME,
				ServerConfiguration.META_DEFAULT_PORT);
		JavaNativeMessageEncoder<ArrayList<String>> encoder = new JavaNativeMessageEncoder<ArrayList<String>>();
		ArrayList<String> sends = new ArrayList<String>();
		sends.add("read");
		sends.add(fileName);
		ByteBuffer bf = encoder.encode(sends);
		session.write(bf);

		int run = 0;
		while (count == 0) {
			if (run == 10)
				break;
			++run;
			Thread.sleep(1000);
		}
		
		if (blocks != null) {
			
			if (blocks.equals("NF")) {
				LOGGER.info("File does not exist");
				count = 0;
				blocks = null; // 清空
				return false;
			}
			
			count = 0;
			blocks = null; // 清空
			return true;
		}

		LOGGER.info("File does not exist");
		return false;
	}

	/***
	 * 是否能写
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	public boolean isWrite(String fileName) throws InterruptedException {

		if (!checkDataNodeIsStart() || !checkNameNodeIsStart()) {
			LOGGER.info("Write failed, please check NameNode or DataNode is start...");
			return false;
		}

		boolean isWrite = DiskInfoUtil.getDiskSize(DATANODEDIR, fileName);
		if (isWrite && !findFile(fileName)) {
//			fileName = DATANODEDIR + "/" + fileName;
//			DataNode dataNode = new DataNode();
//			dataNode.wirte(fileName);
//			LOGGER.info("write file success !");
			return true;
		}

		LOGGER.info("Check whether the parameter is null ");
		return false;
	}
	
	public boolean write(String fileName, DataNode dataNode) {
		try {
			
//			fileName = DATANODEDIR + "/" + fileName;
			dataNode.wirte(fileName);
			LOGGER.info("write file success !");
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	/***
	 * 是否可读
	 * 
	 * @param fileName
	 * @return
	 * @throws InterruptedException
	 */
	public boolean isRead(String fileName) throws InterruptedException {

		if (!checkDataNodeIsStart() || !checkNameNodeIsStart()) {
			LOGGER.info("Write failed, please check NameNode or DataNode is start...");
			return false;
		}

		if (check(fileName) && findFile(fileName)) {
			// fileName = DATANODEDIR + "/" + fileName;
			// DataNode dataNode = new DataNode();
			// dataNode.read(fileName);
			return true;
		} else {
			LOGGER.info("Check whether the parameter is null or fileName not exists");
			return false;
		}
	}

	/**
	 * 读取内容
	 * 
	 * @param fileName
	 * @param dataNode
	 * @return
	 */
	public String read(String fileName, DataNode dataNode) {
		fileName = DATANODEDIR + "/" + fileName;
		String data = dataNode.read(fileName);
		return data;
	}

	/***
	 * 判断文件是否存在
	 * 
	 * @param fileName
	 * @return
	 */
	public boolean fileExists(String fileName) {
		boolean check = check(fileName);

		if (check) {
			List<String> datanodes = meta.get(fileName);
			if (datanodes != null) { // 存在文件
				return true;
			} else {
				return false;
			}
		}

		return false;
	}

	/***
	 * 初始化
	 */
	public void init() {
		if (isInit)
			return;

		if (check(NAMENODEDIR)) {
			isInit = InitServer.initServer(NAMENODEDIR);
		}
	}
}

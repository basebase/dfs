package cn.base.dfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.mina.api.AbstractIoHandler;
import org.apache.mina.api.IoSession;
import org.apache.mina.codec.IoBuffer;
import org.apache.mina.codec.delimited.serialization.JavaNativeMessageDecoder;
import org.apache.mina.codec.delimited.serialization.JavaNativeMessageEncoder;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.nio.NioTcpClient;
import org.apache.mina.transport.nio.NioTcpServer;

import cn.base.dfs.client.DFSClient;
import cn.base.dfs.configuration.ProperConf;
import cn.base.dfs.configuration.ServerConfiguration;
import cn.base.dfs.server.datanode.DataNode;
import cn.base.dfs.server.namenode.NameNode;
import cn.base.dfs.server.namenode.meta.MetaServerConnection;
import cn.base.dfs.status.NodeStatus;

/***
 * NameNode和DataNode的启动类
 * 
 * @author Joker
 *
 */
public class Start implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = Logger.getLogger(Start.class);
	private static final String NAMENODEDIR = (String) ProperConf.configuration.get("dfs.namenode.name.dir");
	private static final String DATANODEDIR = (String) ProperConf.configuration.get("dfs.datanode.data.dir");
	private static final String BLOCKSIZE = (String) ProperConf.configuration.get("dfs.datanode.block.size");
	
	public NameNode nameNode = null;
	public DataNode dataNode = null;

	private static boolean nameNodeRun = false;
	private static boolean dataNodeRun = false;

	public static final String SUCCESS_FILE_NAME = "_SUCCESS";

	public static void main(String[] args) throws IOException {
		Start start = new Start();

		start.startNameNode(true);
		start.startDataNode(true);

		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				if (nameNodeRun) {
//					String nameNodeDir = (String) ProperConf.configuration.get("dfs.namenode.name.dir");
					delSuccessFile(NAMENODEDIR, SUCCESS_FILE_NAME);
				}

				if (dataNodeRun) {
//					String dataNodeDir = (String) ProperConf.configuration.get("dfs.datanode.data.dir");
					delSuccessFile(DATANODEDIR, SUCCESS_FILE_NAME);
				}
			}
		});

		final NioTcpServer acceptor = new NioTcpServer();
		start.requestServer(acceptor, start);
		// 让程序不退出, 不同于while(true)
		// 回车退出;
		new BufferedReader(new InputStreamReader(System.in)).readLine();
		acceptor.unbind();
	}

	/**
	 * 请求节点实例
	 * 
	 * @param acceptor
	 */
	public void requestServer(NioTcpServer acceptor, Start start) {
		// 提供当前NAMENODE和DATANODE的实例
		final SocketAddress address = new InetSocketAddress(ServerConfiguration.NODE_PORT);

		acceptor.setIoHandler(new AbstractIoHandler() {
			@Override
			public void messageReceived(IoSession session, Object message) {
				if (message instanceof ByteBuffer) {
					ByteBuffer bf = (ByteBuffer) message;
					IoBuffer ioBuffer = IoBuffer.wrap(bf);
					JavaNativeMessageDecoder<String> decoder = new JavaNativeMessageDecoder<String>();
					String request = decoder.decode(ioBuffer);
					if (request != null) {
						// JavaNativeMessageEncoder<HashMap<NameNode, DataNode>>
						// encoder = new
						// JavaNativeMessageEncoder<HashMap<NameNode,
						// DataNode>>();
						// HashMap<NameNode, DataNode> m = new HashMap<NameNode,
						// DataNode>();

						JavaNativeMessageEncoder<Start> encoder = new JavaNativeMessageEncoder<Start>();

						// m.put(nameNode, dataNode);
						// ByteBuffer buff = encoder.encode(m);
						ByteBuffer buff = encoder.encode(start);
						session.write(buff);
					}
				}
			}

			@Override
			public void messageSent(IoSession session, Object message) {
				LOGGER.info("Start sent message => " + message);
			}
		});

		acceptor.setFilters(new LoggingFilter("LoggingFilterStart"));
		acceptor.bind(address);
	}

	public void createSuccessFile(String baseDir, String successFileName) {
		File successFile = new File(baseDir, successFileName);
		try {
			successFile.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void delSuccessFile(String baseDir, String successFileName) {
		File delFile = new File(baseDir, successFileName);
		if (delFile.exists()) {
			delFile.delete();
			LOGGER.info("delete " + successFileName + " success !");
		} else {
			LOGGER.info("File does not exist...");
		}
	}

	public boolean startNameNode(boolean isStart) {

		try {

			if (isStart) {
				nameNode = new NameNode();
				nameNode.init();

//				String nameNodeDir = (String) ProperConf.configuration.get("dfs.namenode.name.dir");
				if (NAMENODEDIR != null && !NAMENODEDIR.equals("")) {
					createSuccessFile(NAMENODEDIR, SUCCESS_FILE_NAME);
					nameNodeRun = true;
					LOGGER.info("NameNode is start success...");
					return true;
				} else {
					LOGGER.info("start NameNode fail check NameNode is Initialize...");
					return false;
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
			nameNodeRun = false;
		}

		return false;
	}

	public boolean startDataNode(boolean isStart) {
		try {

			if (isStart) {
				dataNode = new DataNode();
				dataNode.setBlockSize(Long.parseLong(BLOCKSIZE));
				dataNode.init();

//				String dataNodeDir = (String) ProperConf.configuration.get("dfs.datanode.data.dir");
				if (DATANODEDIR != null && !DATANODEDIR.equals("")) {
					createSuccessFile(DATANODEDIR, SUCCESS_FILE_NAME);
					dataNodeRun = true;
					LOGGER.info("DataNode is start success...");
					return true;
				} else {
					LOGGER.info("start DataNode fail check NameNode is Initialize...");
					return false;
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
			dataNodeRun = false;
		}

		return false;
	}

	private Start() {
	}

	public NameNode getNameNode() {
		return nameNode;
	}

	public void setNameNode(NameNode nameNode) {
		this.nameNode = nameNode;
	}

	public DataNode getDataNode() {
		return dataNode;
	}

	public void setDataNode(DataNode dataNode) {
		this.dataNode = dataNode;
	}
}

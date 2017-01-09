package cn.base.dfs.client;

import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.mina.api.AbstractIoHandler;
import org.apache.mina.api.IoSession;
import org.apache.mina.codec.IoBuffer;
import org.apache.mina.codec.delimited.serialization.JavaNativeMessageDecoder;
import org.apache.mina.codec.delimited.serialization.JavaNativeMessageEncoder;
import org.apache.mina.transport.nio.NioTcpClient;

import cn.base.dfs.Start;
import cn.base.dfs.common.MetaServerCheckStart;
import cn.base.dfs.configuration.ServerConfiguration;
import cn.base.dfs.server.datanode.DataNode;
import cn.base.dfs.server.namenode.NameNode;
import cn.base.dfs.server.namenode.meta.MetaServer;
import cn.base.dfs.server.namenode.meta.MetaServerConnection;
import cn.base.dfs.status.NodeStatus;

public class DFSClient {
	
	private NameNode nameNode = null;
	private DataNode dataNode = null;
	
	int count = 0;
	public void init() throws InterruptedException {
		boolean nodeServerIsStart = MetaServerCheckStart.nodeServerIsStart();
		if (!nodeServerIsStart)
			return ;
		
		NioTcpClient client = new NioTcpClient();
		client.setIoHandler(new AbstractIoHandler() {
			@Override
			public void messageReceived(IoSession session, Object message) {		
				if (message instanceof ByteBuffer) {
					ByteBuffer buff = (ByteBuffer) message;
					IoBuffer ioBuffer = IoBuffer.wrap(buff);
					
					JavaNativeMessageDecoder<Start> decoder = new JavaNativeMessageDecoder<Start>();
					Start start = decoder.decode(ioBuffer);
					nameNode = start.getNameNode();
					dataNode = start.getDataNode();
					++count;
				}
			}
		});
		
		IoSession session = MetaServerConnection.getSession(client, ServerConfiguration.HOSTNAME, ServerConfiguration.NODE_PORT);
		String key = "get";
		JavaNativeMessageEncoder<String> encoder = new JavaNativeMessageEncoder<String>();
		ByteBuffer bf = encoder.encode(key);
		session.write(bf);
		
		while (count == 0) {
			Thread.sleep(1000);
		}
		
		return ;
	}
	
	
	public void readData(String fileName) throws InterruptedException {
		init();
		boolean read = nameNode.isRead(fileName);
		if (read) {
			String data = nameNode.read(fileName, dataNode);
			System.out.println(data);
			return;
		}
	}
	
	public void writeData(String fileName, String target) throws InterruptedException {
		init();
		boolean write = nameNode.isWrite(fileName);
		if (write) {
			nameNode.write(fileName, target, dataNode);
		}
	}
	
	public List<String> listFiles(String path) throws InterruptedException {
		init();
		List<String> fileList = nameNode.fileList(path);
		fileList.remove("_SUCCESS");
		for (String file : fileList) {
			System.out.print(file + ",");
		}
		return fileList;
	}
	
	public static void main(String[] args) throws InterruptedException {
		DFSClient client = new DFSClient();
//		client.readData("c");
//		client.writeData("/Users/Joker/Desktop/d", "c");
		client.listFiles("/c");
	}
}

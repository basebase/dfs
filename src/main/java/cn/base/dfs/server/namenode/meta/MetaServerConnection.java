package cn.base.dfs.server.namenode.meta;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.apache.mina.api.AbstractIoHandler;
import org.apache.mina.api.IoFuture;
import org.apache.mina.api.IoHandler;
import org.apache.mina.api.IoSession;
import org.apache.mina.transport.nio.NioTcpClient;
import org.apache.mina.transport.tcp.AbstractTcpClient;

import cn.base.dfs.configuration.ServerConfiguration;

public class MetaServerConnection {

	private static final Logger LOG = Logger.getLogger(MetaServerConnection.class);

	/***
	 * 获取到客户端的连接
	 * 
	 * @param client
	 * @param hostname
	 * @param port
	 * @return
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public static IoSession getSession(NioTcpClient client, String hostname, Integer port) {
		try {
			IoFuture<IoSession> future = client.connect(new InetSocketAddress(hostname, port));
			IoSession session = future.get();
			return session;
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		return null;
	}
}

package cn.base.dfs.server.namenode.meta;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.mina.api.AbstractIoHandler;
import org.apache.mina.api.IoSession;
import org.apache.mina.codec.IoBuffer;
import org.apache.mina.codec.delimited.serialization.JavaNativeMessageDecoder;
import org.apache.mina.codec.delimited.serialization.JavaNativeMessageEncoder;
import org.apache.mina.transport.nio.NioTcpClient;
import org.junit.Test;

import cn.base.dfs.common.MetaServerCheckStart;
import cn.base.dfs.configuration.ServerConfiguration;

public class MetaServerTest {

	private static final Logger LOG = Logger.getLogger(MetaServerTest.class);

	Boolean val = null;

	@Test
	public void write() throws InterruptedException {
		boolean metaServerIsStart = MetaServerCheckStart.metaServerIsStart();
		if (!metaServerIsStart)
			return;

		NioTcpClient client = new NioTcpClient();
		client.setIoHandler(new AbstractIoHandler() {

			@Override
			public void sessionOpened(IoSession session) {
				LOG.info("client session opened");
			}

			@Override
			public void sessionClosed(IoSession session) {
				LOG.info("client session closed");
			}

			@Override
			public void messageReceived(IoSession session, Object message) {
				if (message instanceof ByteBuffer) {
					ByteBuffer bf = (ByteBuffer) message;
					IoBuffer ioBuffer = IoBuffer.wrap(bf);

					JavaNativeMessageDecoder<Boolean> decoder = new JavaNativeMessageDecoder<Boolean>();
					val = decoder.decode(ioBuffer);
					++count;
				}
			}

			@Override
			public void messageSent(IoSession session, Object message) {
				LOG.info("sent message => " + message);
			}
		});
		IoSession session = MetaServerConnection.getSession(client, ServerConfiguration.HOSTNAME,
				ServerConfiguration.META_DEFAULT_PORT);
		JavaNativeMessageEncoder<HashMap<String, ArrayList<String>>> encoder = new JavaNativeMessageEncoder<HashMap<String, ArrayList<String>>>();
		HashMap<String, ArrayList<String>> m = new HashMap<String, ArrayList<String>>();
		ArrayList<String> metaData = new ArrayList<String>();
		metaData.add("A_1");
		metaData.add("A_2");
		metaData.add("A_3");
		m.put("A", metaData);

		ByteBuffer bf = encoder.encode(m);
		session.write(bf);

		while (count == 0) {
			Thread.sleep(1000);
		}

		System.out.println("val = " + val);
		val = null;
		count = 0;
	}

	String blocks = null;
	int count;

	@Test
	public void read() throws InterruptedException {
		boolean metaServerIsStart = MetaServerCheckStart.metaServerIsStart();
		if (!metaServerIsStart)
			return;

		NioTcpClient client = new NioTcpClient();
		client.setIoHandler(new AbstractIoHandler() {

			@Override
			public void sessionOpened(IoSession session) {
				LOG.info("client session opened...");
			}

			@Override
			public void sessionClosed(IoSession session) {
				LOG.info("client session closed...");
			}

			@Override
			public void messageReceived(IoSession session, Object message) {
				if (message instanceof ByteBuffer) {
					ByteBuffer bf = (ByteBuffer) message;
					IoBuffer ioBuffer = IoBuffer.wrap(bf);
					JavaNativeMessageDecoder<String> decoder = new JavaNativeMessageDecoder<String>();
					blocks = decoder.decode(ioBuffer);
					++count;
				}
			}

			@Override
			public void messageSent(IoSession session, Object message) {
				LOG.info("sent data => " + message);
			}
		});

		IoSession session = MetaServerConnection.getSession(client, ServerConfiguration.HOSTNAME,
				ServerConfiguration.META_DEFAULT_PORT);
		JavaNativeMessageEncoder<ArrayList<String>> encoder = new JavaNativeMessageEncoder<ArrayList<String>>();
		ArrayList<String> sendData = new ArrayList<String>();
		sendData.add("read");
		sendData.add("A");

		ByteBuffer bf = encoder.encode(sendData);
		session.write(bf);

		while (count == 0) {
			Thread.sleep(1000);
		}

		System.out.println("blocks = " + blocks);
		count = 0;
		blocks = null;
	}

}

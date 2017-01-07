package cn.base.dfs.test.socket;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.mina.api.IdleStatus;
import org.apache.mina.api.IoHandler;
import org.apache.mina.api.IoService;
import org.apache.mina.api.IoSession;
import org.apache.mina.codec.IoBuffer;
import org.apache.mina.codec.delimited.serialization.JavaNativeMessageDecoder;
import org.apache.mina.codec.delimited.serialization.JavaNativeMessageEncoder;
import org.apache.mina.util.ByteBufferDumper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.bytes.HeapBytesStore;

public class ServerHandler implements IoHandler {

	private static final Logger LOG = LoggerFactory.getLogger(ServerHandler.class);

	@Override
	public void sessionOpened(IoSession session) {

		// LOG.info("cccsession opened {" + session + "}");
		//
		// final String welcomeStr = "welcome\n";
		// final ByteBuffer bf = ByteBuffer.allocate(welcomeStr.length());
		// bf.put(welcomeStr.getBytes());
		// bf.flip();
		// session.write(bf);
	}

	@Override
	public void sessionClosed(IoSession session) {
		System.out.println("IP:" + session.getRemoteAddress().toString() + "断开连接");
	}

	@Override
	public void sessionIdle(IoSession session, IdleStatus status) {

	}

	@Override
	public void messageReceived(IoSession session, Object message) {
		
		System.out.println("111");

		try {
			
			JavaNativeMessageDecoder<HashMap<String, HashMap<String, ArrayList<String>>>> decoder =
					new JavaNativeMessageDecoder<HashMap<String, HashMap<String, ArrayList<String>>>>();
			IoBuffer ioBuff = IoBuffer.wrap((ByteBuffer) message);
			HashMap<String, HashMap<String, ArrayList<String>>> decode = decoder.decode(ioBuff);
			
			
			
			for (Map.Entry<String, HashMap<String, ArrayList<String>>> m1 : decode.entrySet()) {
				System.out.println("m1 k = " + m1.getKey());
				HashMap<String,ArrayList<String>> valueMap = m1.getValue();
				for (Map.Entry<String, ArrayList<String>> m2 : valueMap.entrySet()) {
					System.out.println("m2 key = " + m2.getKey());
					ArrayList<String> valLists = m2.getValue();
					System.out.print("m2 value = ");
					for (String s : valLists) {
						System.out.print(s + ",");
					}
				}
			}
			
//			System.out.println("server message => " + message);
		} catch (Exception e) {
			e.printStackTrace();
		}
		

//		String str = message.toString();
//
//		System.out.println("接受到的消息:" + str);
//
//		if (str.trim().equalsIgnoreCase("quit")) {
//			session.close(true);
//			return;
//		}
//		Date date = new Date();
//		session.write(date.toString());
//		System.out.println("Message written...");
	}

	@Override
	public void messageSent(IoSession session, Object message) {
		System.out.println("发送信息:" + message.toString());
	}

	@Override
	public void serviceActivated(IoService service) {

	}

	@Override
	public void serviceInactivated(IoService service) {

	}

	@Override
	public void exceptionCaught(IoSession session, Exception cause) {

	}

	@Override
	public void handshakeStarted(IoSession abstractIoSession) {

	}

	@Override
	public void handshakeCompleted(IoSession session) {

	}

	@Override
	public void secureClosed(IoSession session) {

	}

}

package cn.base.dfs.test.socket;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

import org.apache.avro.reflect.AvroEncode;
import org.apache.avro.reflect.CustomEncoding;
import org.apache.mina.api.IoFuture;
import org.apache.mina.api.IoSession;
import org.apache.mina.codec.ProtocolEncoder;
import org.apache.mina.codec.delimited.serialization.JavaNativeMessageDecoder;
import org.apache.mina.codec.delimited.serialization.JavaNativeMessageEncoder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.nio.NioTcpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyClient {

	static final private Logger LOG = LoggerFactory.getLogger(MyClient.class);

	public static void main(String[] args) {

		LOG.info("starting echo client");
		final NioTcpClient client = new NioTcpClient();
		client.setFilters(new LoggingFilter("LoggingFilter1")); 
		client.setIoHandler(new ClientHandler());
		
		
		

		try {

			IoFuture<IoSession> future = client.connect(new InetSocketAddress("localhost", 9999));

			try {

				IoSession session = future.get();
				LOG.info("session connected success ");

				Scanner sc = new Scanner(System.in);

				boolean quit = false;

//				while (!quit) {
//
//					String str = sc.next();
//					if (str.equalsIgnoreCase("quit")) {
//						quit = true;
//					}
//					
//					ByteBuffer bf = ByteBuffer.allocate(str.length());
//					bf.put(str.getBytes());
//					bf.flip();
//					session.write(bf);
//				}
				
				HashMap<String, ArrayList<String>> m = new HashMap<String, ArrayList<String>>();
				ArrayList<String> arrayList = new ArrayList<String>();
				arrayList.add("11");
				arrayList.add("12");
				arrayList.add("13");
				m.put("A", arrayList);
				
				HashMap<String, HashMap<String, ArrayList<String>>> putMap = new HashMap<String, HashMap<String, ArrayList<String>>>();
				putMap.put("Joker", m);
				
				JavaNativeMessageEncoder<HashMap<String, HashMap<String, ArrayList<String>>>> in = new JavaNativeMessageEncoder<HashMap<String, HashMap<String, ArrayList<String>>>>();
				ByteBuffer encode = in.encode(putMap);
				session.write(encode);
				
			}

			 catch (ExecutionException e) {
				LOG.error("cannot connect : ", e);
			}

			LOG.debug("Running the client for 10 sec");
			Thread.sleep(50000);
		} catch (Exception e) {
			
		}
	}
}

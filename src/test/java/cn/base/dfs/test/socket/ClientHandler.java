package cn.base.dfs.test.socket;

import java.util.Date;

import org.apache.mina.api.IdleStatus;
import org.apache.mina.api.IoHandler;
import org.apache.mina.api.IoService;
import org.apache.mina.api.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientHandler implements IoHandler {

	static final private Logger LOG = LoggerFactory.getLogger(ClientHandler.class);

	@Override
	public void sessionOpened(IoSession session) {
		System.out.println("打开连接");
	}

	@Override
	public void sessionClosed(IoSession session) {
		LOG.info("client :" + session.getRemoteAddress().toString() + " close connection");
	}

	@Override
	public void sessionIdle(IoSession session, IdleStatus status) {
		// TODO Auto-generated method stub

	}

	@Override
	public void messageReceived(IoSession session, Object message) {
		System.out.println("client接受信息:"+message.toString());  
	}

	@Override
	public void messageSent(IoSession session, Object message) {
		System.out.println("client发送信息"+message.toString());  
	}

	@Override
	public void serviceActivated(IoService service) {
		// TODO Auto-generated method stub

	}

	@Override
	public void serviceInactivated(IoService service) {
		// TODO Auto-generated method stub

	}

	@Override
	public void exceptionCaught(IoSession session, Exception cause) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handshakeStarted(IoSession abstractIoSession) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handshakeCompleted(IoSession session) {
		// TODO Auto-generated method stub

	}

	@Override
	public void secureClosed(IoSession session) {
		// TODO Auto-generated method stub

	}

}

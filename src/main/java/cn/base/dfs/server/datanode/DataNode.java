package cn.base.dfs.server.datanode;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.mina.api.AbstractIoHandler;
import org.apache.mina.api.IoSession;
import org.apache.mina.codec.IoBuffer;
import org.apache.mina.codec.delimited.serialization.JavaNativeMessageDecoder;
import org.apache.mina.codec.delimited.serialization.JavaNativeMessageEncoder;
import org.apache.mina.transport.nio.NioTcpClient;

import cn.base.dfs.common.InitServer;
import cn.base.dfs.common.MetaServerCheckStart;
import cn.base.dfs.configuration.ProperConf;
import cn.base.dfs.configuration.ServerConfiguration;
import cn.base.dfs.server.namenode.NameNode;
import cn.base.dfs.server.namenode.meta.MetaServerConnection;
import cn.base.dfs.test.MegerRunnable;

public class DataNode implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = Logger.getLogger(DataNode.class);

	private static NameNode nameNode = new NameNode();

	private String fileName;
	private String blockName;
	private long blockSize;
	private long start;
	private long end;

	private AtomicInteger atomicInteger = new AtomicInteger(0);
	private AtomicInteger readAtomicInteger = new AtomicInteger(0);

	private boolean isInit = false;

	public boolean isCheck(String str) {
		if (str == null || str.equals("")) {
			return false;
		}
		return true;
	}

	/**
	 * 当dataNode成功写入数据调用写入元数据信息
	 * 
	 * @param originFileName
	 * @param metas
	 */
	Boolean val = null;
	int count = 0;

	public void nameNodeCallback(String originFileName, ArrayList<String> metas) throws InterruptedException {
		nameNode.meta.put(originFileName, metas);

		if (!MetaServerCheckStart.metaServerIsStart()) {
			LOGGER.info("meta write metaServer faild check metaServer is start...");
			return;
		}

		NioTcpClient client = new NioTcpClient();
		client.setIoHandler(new AbstractIoHandler() {

			@Override
			public void sessionOpened(IoSession session) {
				LOGGER.info("client session opened");
			}

			@Override
			public void sessionClosed(IoSession session) {
				LOGGER.info("client session closed");
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
				LOGGER.info("sent message => " + message);
			}
		});

		IoSession session = MetaServerConnection.getSession(client, ServerConfiguration.HOSTNAME,
				ServerConfiguration.META_DEFAULT_PORT);
		JavaNativeMessageEncoder<HashMap<String, ArrayList<String>>> encoder = new JavaNativeMessageEncoder<HashMap<String, ArrayList<String>>>();
		HashMap<String, ArrayList<String>> m = new HashMap<String, ArrayList<String>>();
		m.put(originFileName, metas);
		ByteBuffer bf = encoder.encode(m);
		session.write(bf);

		while (count == 0) {
			Thread.sleep(1000);
		}

		System.out.println("meta write => " + val);
		val = null;
		count = 0;
	}

	public void init() {
		if (isInit)
			return;

		String dataNodeDir = (String) ProperConf.configuration.get("dfs.datanode.data.dir");
		if (isCheck(dataNodeDir)) {
			isInit = InitServer.initServer(dataNodeDir);
		}
	}

	/***
	 * 读取文件内容
	 * 
	 * @param fileName
	 */
	public String read(String fileName) {
		if (isCheck(fileName)) {

			try {

				File file = new File(fileName);
				if (!file.isDirectory()) {
					LOGGER.info(fileName + " is not Directory your check write is suceess !");
					return null;
				} else {

					File[] files = file.listFiles();
					ArrayList<File> mergeFiles = new ArrayList<File>();
					for (File f : files) {
						if (!f.getName().endsWith(".meta")) {
							mergeFiles.add(f);
						}
					}

					if (mergeFiles == null || mergeFiles.size() == 0) {
						LOGGER.info("Check the " + fileName + " file exists !");
						return null;
					}

					Collections.sort(mergeFiles, new FileComparator());

					long start = 0;
					long end = 0;
					String mergeFileName = fileName + "_tmp";
					RandomAccessFile randomAccessFile = new RandomAccessFile(mergeFileName, "rw");
					randomAccessFile.setLength(
							blockSize * (mergeFiles.size() - 1) + mergeFiles.get(mergeFiles.size() - 1).length());
					randomAccessFile.close();

					for (int i = 0; i < mergeFiles.size(); i++) {
						start = end;
						end = (i + 1) * blockSize;
						ReadThread readThread = new ReadThread(start, mergeFileName, mergeFiles.get(i));
						Thread t = new Thread(readThread);
						t.start();
					}

					while (readAtomicInteger.get() != mergeFiles.size()) {
						Thread.sleep(1000);
					}

					BufferedReader br = new BufferedReader(new FileReader(mergeFileName));
					String line = null;
					StringBuffer buff = new StringBuffer();
					while ((line = br.readLine()) != null) {
						buff.append(line + "\n");
					}

					if (new File(mergeFileName).exists()) {
						LOGGER.info("tmp merge file delet path -> " + new File(mergeFileName).getPath());
						new File(mergeFileName).delete();
					}

					// LOGGER.info(buff);
					return buff.toString();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	/***
	 * 将文件写入本地
	 * 
	 * @param fileName
	 * @throws InterruptedException
	 */
	public void wirte(String fileName, String target) throws InterruptedException {
		String dataNodeDir = (String) ProperConf.configuration.get("dfs.datanode.data.dir");
		if (isCheck(dataNodeDir)) {
			File originFile = new File(fileName);
			long length = originFile.length();
			// 线城数量
			int count = (int) Math.ceil(length / (double) blockSize);
			// 文件最后一点数据
			int remainder = (int) (length % blockSize);

			long start = 0;
			long end = 0;

			String writeHome = null;
			try {

				String dirName = originFile.getName();
				writeHome = null;// dataNodeDir + "/" + dirName;

				if (target != null && !target.equals("")) {
					writeHome = dataNodeDir + "/" + target + "/" + dirName;
				} else {
					writeHome = dataNodeDir + "/" + dirName;
				}
				
				File dirFile = new File(writeHome);
				if (!dirFile.exists()) {
					dirFile.mkdirs();
					LOGGER.info("create write file " + writeHome + " Directory success !");
				} else {
					LOGGER.info("write file Directory already exists !");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			ArrayList<String> writeFiles = new ArrayList<String>();

			for (int i = 0; i < count; i++) {
				this.blockName = writeHome + "/" + originFile.getName() + "_" + (i + 1);
				String threadName = originFile.getName() + "_" + (i + 1);
				String metaName = writeHome + "/" + originFile.getName() + "_" + (i + 1) + ".meta";
				String blockID = "block_" + (i + 1);

				writeFiles.add(blockName);

				if (i == 0) {
					start = end;
					end = (i + 1) * blockSize;
					WriteThread writeThread = new WriteThread(blockID, start, end, blockSize, blockName, metaName,
							originFile);
					new Thread(writeThread, threadName).start();
				} else if (i == count - 1) {

					start = end;
					end = start + remainder;
					WriteThread writeThread = new WriteThread(blockID, start, end, blockSize, blockName, metaName,
							originFile);
					new Thread(writeThread, threadName).start();
				} else {

					start = end;
					end = (i + 1) * blockSize;
					WriteThread writeThread = new WriteThread(blockID, start, end, blockSize, blockName, metaName,
							originFile);
					new Thread(writeThread, threadName).start();
				}
			}

			while (atomicInteger.get() != count) {
				Thread.sleep(1000);
			}

			LOGGER.info("all block write complete !");
			this.nameNodeCallback(originFile.getName(), writeFiles);

		} else {
			LOGGER.info("datanode dir is not exists");
		}
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getBlockName() {
		return blockName;
	}

	public void setBlockName(String blockName) {
		this.blockName = blockName;
	}

	public long getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(long blockSize) {
		this.blockSize = blockSize;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public boolean isInit() {
		return isInit;
	}

	public void setInit(boolean isInit) {
		this.isInit = isInit;
	}

	/***
	 * 多线程写文件
	 * 
	 * @author Joker
	 *
	 */
	class WriteThread implements Runnable {

		private long start;
		private long end;
		private String block_id;
		private long blockSize;
		private String blockName;
		private String metaName;
		private File originFile;

		private DataNodeMeta dataNodeMeta = null;

		public WriteThread(long start, long blockSize, String blockName, File originFile) {
			this.start = start;
			this.blockSize = blockSize;
			this.blockName = blockName;
			this.originFile = originFile;
		}

		public WriteThread(String block_id, long start, long end, long blockSize, String blockName, File originFile) {
			this.block_id = block_id;
			this.start = start;
			this.end = end;
			this.blockSize = blockSize;
			this.blockName = blockName;
			this.originFile = originFile;
		}

		public WriteThread(String block_id, long start, long end, long blockSize, String blockName, String metaName,
				File originFile) {
			this.block_id = block_id;
			this.start = start;
			this.end = end;
			this.blockSize = blockSize;
			this.blockName = blockName;
			this.metaName = metaName;
			this.originFile = originFile;
		}

		@Override
		public void run() {

			RandomAccessFile accessFile = null;
			OutputStream os = null;

			ObjectOutputStream oos = null;
			FileOutputStream fos = null;

			try {

				// 元数据
				dataNodeMeta = new DataNodeMeta();
				fos = new FileOutputStream(metaName);
				oos = new ObjectOutputStream(fos);

				accessFile = new RandomAccessFile(originFile, "r");
				byte[] read = new byte[(int) blockSize];
				accessFile.seek(start);
				int readLen = accessFile.read(read);
				os = new FileOutputStream(blockName);
				os.write(read, 0, readLen);
				os.flush();

			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {

					if (os != null)
						os.close();
					if (accessFile != null)
						accessFile.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

				atomicInteger.getAndIncrement();

				try {

					if (oos != null) {
						dataNodeMeta.setBlock_id(block_id);
						dataNodeMeta.setFileName(originFile.getName());
						dataNodeMeta.setStart(start);
						dataNodeMeta.setEnd(end);
						dataNodeMeta.write(oos);
						oos.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

				LOGGER.info("DataNode Meta Info: " + dataNodeMeta.toString());
				LOGGER.info(Thread.currentThread().getName() + " spill " + blockName + " complete !");
			}
		}
	}

	class ReadThread implements Runnable {

		private RandomAccessFile accessFile = null;
		private long start;
		private String mergerFileName;
		private File partFile;
		private FileInputStream fis = null;

		public ReadThread(long start, String mergeFileName, File partFile) {
			this.start = start;
			this.mergerFileName = mergeFileName;
			this.partFile = partFile;
		}

		@Override
		public void run() {

			try {

				accessFile = new RandomAccessFile(mergerFileName, "rw");
				fis = new FileInputStream(partFile);
				int available = fis.available();
				byte[] bs = new byte[available];
				accessFile.seek(start);
				fis.read(bs);
				accessFile.write(bs);

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {

					if (accessFile != null)
						accessFile.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

				readAtomicInteger.getAndIncrement();
				LOGGER.info(Thread.currentThread().getName() + " read " + mergerFileName + " complete !");
			}
		}
	}

	private class FileComparator implements Comparator<File> {
		@Override
		public int compare(File o1, File o2) {
			return o1.getName().compareToIgnoreCase(o2.getName());
		}
	}
}

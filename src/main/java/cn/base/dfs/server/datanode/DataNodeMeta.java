package cn.base.dfs.server.datanode;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class DataNodeMeta implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
//	private static final String SERIALIZABLE_FILE_DEFAULT_PATH = "";
	
	private String fileName;
	private String block_id;
	private long start;
	private long end;
	
	/***
	 * 序列化到文件
	 * @throws IOException 
	 */
	public void write(ObjectOutputStream outputStream) throws IOException {
		outputStream.writeUTF(fileName);
		outputStream.writeUTF(block_id);
		outputStream.writeLong(start);
		outputStream.writeLong(end);
	}
	
	/***
	 * 反序列化
	 * @throws IOException 
	 */
	public DataNodeMeta read(ObjectInputStream inputStream) throws IOException {
		String fileName = inputStream.readUTF();
		String block_id = inputStream.readUTF();
		long start = inputStream.readLong();
		long end = inputStream.readLong();
		
		DataNodeMeta meta = new DataNodeMeta();
		meta.setFileName(fileName);
		meta.setBlock_id(block_id);
		meta.setStart(start);
		meta.setEnd(end);
		return meta;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getBlock_id() {
		return block_id;
	}

	public void setBlock_id(String block_id) {
		this.block_id = block_id;
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

	@Override
	public String toString() {
		return "DataNodeMeta [fileName=" + fileName + ", block_id=" + block_id + ", start=" + start + ", end=" + end
				+ "]";
	}
}

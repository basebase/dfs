package cn.base.dfs.server.datanode;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import cn.base.dfs.server.namenode.NameNode;

public class DataNodeTest {
	
	private DataNode dataNode = null;
	
	@Before
	public void init() {
		dataNode = new DataNode();
		dataNode.init();
		dataNode.setBlockSize(500);
	}
	
	@Test
	public void write() throws InterruptedException {
//		dataNode.wirte("/Users/Joker/Desktop/b");
//		Map<String, List<String>> meta = NameNode.meta;
//		for (Map.Entry<String, List<String>> m : meta.entrySet()) {
//			System.out.println("k = " + m.getKey() + " v = " + m.getValue() );
//		}
	}
	
	
	
	@Test
	public void read() {
		dataNode.read("/Users/Joker/Desktop/dfs/datanode/b");
	}

}

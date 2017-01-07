package cn.base.dfs.server.namenode;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class NameNodeTest {

	private NameNode nameNode = null;

	@Before
	public void init() {
		nameNode = new NameNode();
		nameNode.init();
	}

	@Test
	public void start() {
//		nameNode.initMeta();
//		Map<String, List<String>> meta = NameNode.meta;
//		for (Map.Entry<String, List<String>> m : meta.entrySet()) {
//			System.out.println("k = " + m.getKey() + " v = " + m.getValue());
//		}
	}

	@Test
	public void testConnection() {
//		boolean metaServerIsStart = nameNode.metaServerIsStart();
//		System.out.println(metaServerIsStart);
	}
	
}

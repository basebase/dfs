package cn.base.dfs;

import org.junit.Test;

import cn.base.dfs.common.MetaServerCheckStart;

public class StartTest {

	
	@Test
	public void isStart() {
		boolean nodeServerIsStart = MetaServerCheckStart.nodeServerIsStart();
		System.out.println(nodeServerIsStart);
	}
}

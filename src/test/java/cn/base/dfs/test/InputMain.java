package cn.base.dfs.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.base.dfs.configuration.ProperConf;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

public class InputMain {

	public static Map<String, List<String>> meta = new HashMap<String, List<String>>();
	static SingleChronicleQueue queue = null;

	public static void main(String[] args) {

		String metaPath = ProperConf.configuration.getProperty("dfs.namenode.meta.dir");

		queue = SingleChronicleQueueBuilder.binary(metaPath).build();
		ExcerptAppender appender = queue.acquireAppender();

//		List<String> list1 = new ArrayList<String>();
//		List<String> list2 = new ArrayList<String>();
//		List<String> list3 = new ArrayList<String>();
//
//		list1.add("1");
//		list1.add("2");
//		list1.add("3");
//
//		list2.add("4");
//		list2.add("5");
//		list2.add("6");
//
//		list3.add("7");
//		list3.add("8");
//		list3.add("9");
//
//		meta.put("h11", list1);
//		meta.put("h22", list2);
//		meta.put("h33", list3);
		meta.remove("h1");

		appender.writeMap(meta);
	}
}

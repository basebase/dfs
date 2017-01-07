package cn.base.dfs.test;

import java.util.List;
import java.util.Map;

import cn.base.dfs.configuration.ProperConf;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

public class OutputMain {

	public static void main(String[] args) {
		String metaPath = ProperConf.configuration.getProperty("dfs.namenode.meta.dir");
		SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(metaPath).build();
		
		ExcerptTailer tailer = queue.createTailer();
		
		Map<String, List<String>> readMap = tailer.readMap();
		
		
		
		System.out.println(readMap);

//		while (true) {
//			Map<String, List<String>> map = tailer.readMap();
//			if (map == null) {
//				Jvm.pause(0);
//				break;
//			} else {
//				for (Map.Entry<String, List<String>> m : map.entrySet()) {
//					System.out.println("k = " + m.getKey() + " v = " + m.getValue());
//				}
//				
//				System.out.println("---------------------------------------------");
//			}
//
//		}
	}
}

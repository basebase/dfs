package cn.base.dfs.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class TestApp {

//	public StringBuffer read(String fileName) throws InterruptedException {
//		StringBuffer buff = new StringBuffer();
//		if (Thread.currentThread().getName().equals("1")) {
//			System.out.println("1 start ...");
//			Thread.sleep(1000);
//			System.out.println("1 end ...");
//		} else if (Thread.currentThread().getName().equals("2")) {
//			System.out.println("2 start ...");
//			Thread.sleep(2000);
//			System.out.println("2 end ...");
//		} else if (Thread.currentThread().getName().equals("3")) {
//			System.out.println("3 start ...");
//			Thread.sleep(3000);
//			System.out.println("3 end ...");
//		} else if (Thread.currentThread().getName().equals("4")) {
//			System.out.println("4 start ...");
//			Thread.sleep(4000);
//			System.out.println("4 end ...");
//		}
//		
//		
//		buff.append(fileName);
//		System.out.println(Thread.currentThread().getName() + " " + buff);
//		return buff;
//	}
//
//	public void readData() throws InterruptedException {
//		List<String> datas = new ArrayList<String>();
//		datas.add("1");
//		datas.add("2");
//		datas.add("3");
//		datas.add("4");
//
//		Map<String, StringBuffer> map = new HashMap<String, StringBuffer>();
//		ThreadGroup group = new ThreadGroup("A");
//		for (final String data : datas) {
//			new Thread(group, new Runnable() {
//
//				@Override
//				public void run() {
//					try {
//						
//						StringBuffer buff = read(data);
//						map.put(Thread.currentThread().getName(), buff);
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
//				}
//			}, data).start();
//		}
//		
//		
//		while (group.activeCount() > 0) {
//			Thread.sleep(1000);
//		}
//		
//		
//		for (Map.Entry<String, StringBuffer> m : map.entrySet()) {
//			System.out.println(m.getKey() + " <-> " + m.getValue().toString());
//		}
//	}
//
//	
//	public Integer maxKey(Set<String> keySet) {
//		List<Integer> keys = new ArrayList<Integer>();
//		if (keySet != null) {
//			Iterator<String> it = keySet.iterator();
//			while (it.hasNext()) {
//				String key = it.next();
//				String[] tokens = key.split("_");
//				keys.add(Integer.parseInt(tokens[1]));
//			}
//		}
//		
//		Collections.sort(keys);
//		
//		return keys.get(keys.size() - 1);
//	}
//	
//	public static void main(String[] args) throws InterruptedException {
////		new TestApp().readData();
//		Map<String, String> map = new HashMap<String, String>();
//		map.put("block_1", "1");
//		map.put("block_2", "2");
//		map.put("block_4", "4");
//		map.put("block_3", "3");
//		map.put("block_10", "10");
//		map.put("block_101", "101");
//		
//		
//		Integer maxKey = new TestApp().maxKey(map.keySet());
//		System.out.println(maxKey);
//	}

}

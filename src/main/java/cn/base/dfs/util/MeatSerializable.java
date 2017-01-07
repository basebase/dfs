package cn.base.dfs.util;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;

public class MeatSerializable {

	public static void writeMapToXML(Map<String, String> map) throws IOException {
		if (map == null || map.size() == 0)
			return ;

		FileOutputStream fos = new FileOutputStream("fsimage");
		ObjectOutputStream output = new ObjectOutputStream(fos);

		XStream magicApi = new XStream();
		magicApi.registerConverter(new MapEntryConverter());
		magicApi.alias("configuration", Map.class);

		Map<String, String> extractedMap = (Map<String, String>) magicApi.fromXML(magicApi.toXML(map));
		output.writeObject(extractedMap);
		return ;
	}

	public static class MapEntryConverter implements Converter {

		@Override
		public boolean canConvert(Class clazz) {
			return AbstractMap.class.isAssignableFrom(clazz);
		}

		@Override
		public void marshal(Object value, HierarchicalStreamWriter writer, MarshallingContext context) {
			AbstractMap map = (AbstractMap) value;
			for (Object obj : map.entrySet()) {
				Map.Entry entry = (Map.Entry) obj;
				writer.startNode(entry.getKey().toString());
				Object val = entry.getValue();
				if (null != val) {
					writer.setValue(val.toString());
				}
				writer.endNode();
			}
		}

		@Override
		public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
			Map<String, String> map = new HashMap<String, String>();

			while (reader.hasMoreChildren()) {
				reader.moveDown();

				String key = reader.getNodeName(); // nodeName aka element's
													// name
				String value = reader.getValue();
				map.put(key, value);

				reader.moveUp();
			}

			return map;
		}

	}
}

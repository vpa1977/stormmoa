package moa.storm.scheme;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

public class InstanceScheme  implements Scheme {
	
	private Fields m_fields;

	public InstanceScheme()
	{
		m_fields = new Fields("instance");
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	
	public List<Object> deserialize(byte[] ser) {
		ArrayList<Object> result = new ArrayList<Object>();
		try {
			ObjectInputStream oStream = new ObjectInputStream(new ByteArrayInputStream(ser));
			Object instance = oStream.readObject();
			result.add(instance);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return result;
	}

	
	public Fields getOutputFields() {
		return m_fields;
	}

}

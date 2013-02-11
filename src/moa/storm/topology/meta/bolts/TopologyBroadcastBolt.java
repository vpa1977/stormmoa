package moa.storm.topology.meta.bolts;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import weka.core.Instance;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class TopologyBroadcastBolt extends BaseRichBolt implements IRichBolt
{
	
	private long m_instance_id;
	private OutputCollector m_collector;
	private int m_ensemble_size;
	int m_task_id;
	private String m_stream_id;
	private List<String> m_fields;
	public TopologyBroadcastBolt(String streamId, List<String> f)
	{
		m_stream_id = streamId;
		ArrayList<String> fields = new ArrayList<String>();
		fields.add("id");
		fields.addAll(f);
		m_fields = fields;
	}
	
	public List<String> getFields()
	{
		return m_fields;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		m_collector = collector;
		m_instance_id = 0;
		int task_id = context.getThisTaskId();
		m_task_id = task_id;
	}

	@Override
	public void execute(Tuple tuple) {
		
		Object value = tuple.getValue(0);
		Object inst = null;
		if (value instanceof byte[]){
			byte[] b = (byte[]) value;
	        ObjectInputStream is;
	        Object serializedObject = null;
			try {
				is = new ObjectInputStream( new ByteArrayInputStream(b));
				serializedObject = is.readObject();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			inst = serializedObject;
		}
		else
		if (value instanceof String){
			byte[] b = DatatypeConverter.parseBase64Binary(String.valueOf(value));
	        ObjectInputStream is;
	        Object serializedObject = null;
			try {
				is = new ObjectInputStream( new ByteArrayInputStream(b));
				serializedObject = is.readObject();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			inst = serializedObject;
		}
		else if (value instanceof List)	{
			inst = value;
		}
		else if (value instanceof Instance) {
			inst = value;
		}
		else{
			throw new RuntimeException("Cannot deserialize "+ value);
		}
		m_instance_id++;
		ArrayList<Object> output = new ArrayList<Object>();
		
		
		
		output.add(new moa.storm.topology.message.MessageIdentifier(m_task_id, m_instance_id));
		output.addAll(tuple.getValues());
		int index = tuple.getFields().fieldIndex("instance");
		output.set(index+1, inst);
		
		m_collector.emit(m_stream_id,tuple,output);
		m_collector.ack(tuple);
		if (m_instance_id % 10000 == 0)
		{
			System.out.println("Deserialized "+ m_instance_id+ " on "+ m_task_id);
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(m_stream_id,new Fields(m_fields));
	}
	
}
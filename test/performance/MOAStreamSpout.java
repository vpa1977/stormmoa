package performance;

import java.io.File;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import moa.storm.topology.message.MessageIdentifier;
import moa.storm.topology.spout.InstanceStreamSource;
import moa.streams.InstanceStream;
import weka.core.Instance;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class MOAStreamSpout extends BaseRichSpout implements IRichSpout, InstanceStreamSource {
	
	class Measurement implements Serializable {
		long count;
		
		long m_start;			

		public Measurement()
		{
			m_start = System.currentTimeMillis();
			count = 0;
			
		}
		
		public void check()
		{
			if (m_start == 0 ) 
				m_start = System.currentTimeMillis();
			long current =System.currentTimeMillis();
			count ++;
			if (current - m_start > 1000 * 60)
			{
				writeResult(current - m_start);
				m_start = System.currentTimeMillis();
				count = 0;
			}  
		}
		private int getPid() throws Throwable
		{
			java.lang.management.RuntimeMXBean runtime = java.lang.management.ManagementFactory.getRuntimeMXBean();
			java.lang.reflect.Field jvm = runtime.getClass().getDeclaredField("jvm");
			jvm.setAccessible(true);
			sun.management.VMManagement mgmt = (sun.management.VMManagement) jvm.get(runtime);
			java.lang.reflect.Method pid_method = mgmt.getClass().getDeclaredMethod("getProcessId");
			pid_method.setAccessible(true);
			int pid = (Integer) pid_method.invoke(mgmt);
			return pid;
		}
		private void writeResult(long period)
		{
			try {
				
				long tup_sec = count * 100 * 1000 /period;
				
				File f = new File("/home/vp37/moa_spout_learn"+ InetAddress.getLocalHost().getHostName() + "-" + getPid());
				FileOutputStream fos = new FileOutputStream(f);
				String result = "" +tup_sec;
				fos.write(result.getBytes());
				fos.write(" \r\n".getBytes());
				fos.flush(); 
				fos.close();
			}
			catch (Throwable t)
			{
				t.printStackTrace();
			}
		}
	}

	

	private static final long serialVersionUID = 2296373075956453004L;
	private long m_acked;
	private SpoutOutputCollector m_collector;
	private long m_delay;
	private long m_id;
	private int m_key;
	private Measurement m_measurement;
	private long m_sent;
	
	private InstanceStream m_stream;

	public MOAStreamSpout(InstanceStream stream, int delay)
	{
		m_stream = stream;
		m_delay = delay;
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		super.ack(msgId);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("instance"));
	}
	
	@Override
	public void nextTuple() {
			if (m_measurement == null)
				m_measurement = new Measurement();
			m_measurement.check();
			if (m_delay == 0 || m_id < m_delay ) 
			{
				List<Object> message = new ArrayList<Object>();
				message.add(read());
				m_collector.emit(message, new MessageIdentifier(m_key,m_id++));
				if (m_id % 10000 == 0 && m_delay == 0)
				{
					System.out.println("Emitted "+m_id+" tuples");
				}
				
			}
		
	}


	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		m_collector = collector;
		m_stream.restart();
		m_id = 0;
		m_acked= 0;
		m_key = context.getThisTaskId();
	}
	
	@Override
	public ArrayList<Instance> read() {
		ArrayList<Instance> instances = new ArrayList<Instance>();
		for (int i = 0 ; i < 100 ; i++)
			instances.add(m_stream.nextInstance());
		return instances;
	}


}

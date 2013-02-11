package moa.storm.topology.spout;

import java.io.File;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;


import moa.storm.persistence.IStateFactory;
import moa.storm.persistence.IPersistentState;
import moa.storm.topology.message.EnsembleCommand;
import moa.storm.topology.message.MessageIdentifier;
import moa.storm.topology.message.Reset;

import storm.trident.state.StateFactory;
import weka.core.Instance;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class LearnSpout extends BaseRichSpout implements IRichSpout {

	public static final List<String> LEARN_STREAM_FIELDS = Arrays.asList( new String[]{"instance", "version"});
	public static final String NOTIFICATION_STREAM = "notification";
	public static final String COMMAND_FIELD = "command";
	private static final int NOTIFICATION_ID = -1;
	public static final String EVENT_STREAM = "events";

	private InstanceStreamSource m_stream_src;
	private IStateFactory m_classifier_state;
	private SpoutOutputCollector m_collector;
	private IPersistentState<String> m_state;
	private long m_version;
	private boolean m_reset;
	private int m_task_id;
	private long m_id;
	private long m_pending;
	private int m_key;

	private Measurement m_measurement;

	public LearnSpout(InstanceStreamSource stream_src, IStateFactory classifierState,
			long pending) {
		m_stream_src = stream_src;
		m_classifier_state = classifierState;
		m_pending = pending;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		m_collector = collector;
		
		m_state = ((IStateFactory) m_classifier_state).create();
		m_version = readVersion(m_state)+1;
		if (m_version <0 )
			m_version =0;
		m_reset = true;
		m_task_id = context.getThisTaskId();
		m_id = 0;
		m_key = context.getThisTaskId();
		
	}


	@Override
	public void nextTuple() {
		if (m_reset) {
			m_collector.emit(
					NOTIFICATION_STREAM,
					new ArrayList<Object>(Arrays
							.asList(new EnsembleCommand[] { new Reset(
									m_version, m_pending) })),
					new MessageIdentifier(NOTIFICATION_ID, m_id++));
			m_reset = false;
		}
		
		List<Object> message = new ArrayList<Object>();
		message.add(m_stream_src.read());
		message.add(m_version);
		m_collector.emit(EVENT_STREAM, message, new MessageIdentifier(m_key,m_version));

	}

	@Override
	public void fail(Object msgId) {
		super.fail(msgId);
		m_reset = true;
	}

	@Override
	public void ack(Object msgId) {
		if (m_measurement == null)
			m_measurement = new Measurement();
		m_measurement.check();

		super.ack(msgId);
		MessageIdentifier msg = (MessageIdentifier) msgId;
		if (msg.getTask() != NOTIFICATION_ID)
		{
			long confirmedVersion = msg.getId();
			if (confirmedVersion % m_pending == 0) 
			{
				updateVersion(m_state, confirmedVersion);
				m_measurement.write();
			}
			m_version++;
		}
	}

	private void updateVersion(IPersistentState state, long l) {
		state.setLong("version", "version", l);
	}

	private long readVersion(IPersistentState state) {
		return state.getLong("version", "version");
	}



	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(EVENT_STREAM,new Fields(LEARN_STREAM_FIELDS));
		declarer.declareStream(NOTIFICATION_STREAM, new Fields(COMMAND_FIELD));
	}
	
	
	
	class Measurement implements Serializable{
		public Measurement()
		{
			m_start = System.currentTimeMillis();
			count = 0;
			
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
				
				File f = new File("/home/vp37/learn"+ InetAddress.getLocalHost().getHostName() + "-" + getPid());
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
		
		public void check()
		{
			if (m_start == 0 ) 
				m_start = System.currentTimeMillis();

			count ++;
		}
		public void write()
		{
			long current =System.currentTimeMillis();
			if (current - m_start > 1000 * 60)
			{
				writeResult(current - m_start);
				m_start = System.currentTimeMillis();
				count = 0;
			}  
		}
		
		long m_start;
		long count;
	}
	
}

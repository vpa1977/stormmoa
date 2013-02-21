package performance;

import java.io.File;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;


import backtype.storm.hooks.ITaskHook;
import backtype.storm.hooks.info.BoltAckInfo;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.hooks.info.BoltFailInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.hooks.info.SpoutFailInfo;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

class CounterBolt extends BaseRichBolt implements IRichBolt, ITaskHook
{
	
	public CounterBolt(HashSet<String> trackedComponents)
	{
		m_tracked_components = trackedComponents;
	}


	private OutputCollector m_collector;
	private HashSet<String> m_tracked_components;
	private TopologyContext m_context;
	
	long m_instance;
	long m_start =0;
	long m_measurement_start = 0;
	long count = 0;
	long period = 0;
	
	final long MEASUREMENT_PERIOD = 1 * 60 * 1000;

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
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


	
	private void writeResult(String prefix,long period)
	{
		try {
			
			long tup_sec = count * LocalStorm.GLOBAL_BATCH_SIZE * 1000 /period;
			File f = new File(System.getProperty("user.home") +File.separator+ prefix+ 
					InetAddress.getLocalHost().getHostName() + "-" + getPid() + "-" + m_instance);
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

	@Override
	public void execute(Tuple tuple) {
		m_collector.ack(tuple);
		measure("counter");
	}

	private void measure(String prefix) {
		if (m_start == 0 ) 
			m_start = System.currentTimeMillis();
		long current =System.currentTimeMillis(); 
		count ++;
		if (count %10000 == 0)
			System.out.println("processed "+ count);
		if (current - m_start > MEASUREMENT_PERIOD)
		{
			LocalStorm.LOG.info("Writing Result");
			writeResult(prefix,current - m_start);
			m_start = System.currentTimeMillis();
			count = 0;
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		m_collector = collector;
		m_instance = LocalStorm.INSTANCE ++;
		System.out.println("New Instance "+ LocalStorm.INSTANCE);
		context.addTaskHook(this);	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prepare(Map conf, TopologyContext context) {
		m_context = context;
	}

	@Override
	public void emit(EmitInfo info) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void spoutAck(SpoutAckInfo info) {
		String component = m_context.getComponentId(info.spoutTaskId);
		if (m_tracked_components.contains(component))
		{
			measure(component);
		}
	}

	@Override
	public void spoutFail(SpoutFailInfo info) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void boltExecute(BoltExecuteInfo info) {
		
	}

	@Override
	public void boltAck(BoltAckInfo info) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void boltFail(BoltFailInfo info) {
		// TODO Auto-generated method stub
		
	}
	
}
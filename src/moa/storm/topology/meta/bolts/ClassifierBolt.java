package moa.storm.topology.meta.bolts;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import moa.classifiers.Classifier;
import moa.core.DoubleVector;
import moa.core.MiscUtils;
import moa.options.ClassOption;
import moa.options.Option;
import weka.core.Instance;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ClassifierBolt extends BaseRichBolt implements IRichBolt
{
	private OutputCollector m_collector;
	private Classifier m_wrapper;
	private String m_cli_string;
	public Random classifierRandom = new Random();
	
	private Classifier getClassifier(String cliString) throws RuntimeException {
		Classifier cls;
		try {
			cls = (Classifier)ClassOption.cliStringToObject(cliString, Classifier.class, new Option[]{});
			cls.prepareForUse();
			cls.resetLearning();
			return cls;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	
	public ClassifierBolt(String cliString)
	{
		m_cli_string = cliString;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		m_collector = collector;
		m_wrapper =  getClassifier(m_cli_string);
	}

	@Override
	public void execute(Tuple tuple) {
		if ("learn".equals(tuple.getSourceStreamId()))
		{
			
			List<Instance> list = (List<Instance>)tuple.getValue(1);
			Iterator<Instance> it = list.iterator();
			while (it.hasNext()) {
				Instance value = it.next();
				int weight =  MiscUtils.poisson(1.0, this.classifierRandom);
				if (weight > 0) {
					Instance trainInst = (Instance) (value).copy();
					trainInst.setWeight(trainInst.weight() * weight);
					m_wrapper.trainOnInstance(trainInst);
				//	m_state.set("classifier"+id,m_wrapper);
				}
			}
		} 
		else
		if ("evaluate".equals(tuple.getSourceStreamId()))
		{
			Object instance_id = tuple.getValue(0);
			Object instance = tuple.getValue(1);
			
			if (m_wrapper != null) {
				List<Object> objs = new ArrayList<Object>();
				objs.add(instance_id);
				objs.add( instance );
				ArrayList< DoubleVector > results = new ArrayList<DoubleVector>();
				List<Instance> list = (List<Instance>) instance;
				Iterator<Instance> it = list.iterator();
				while (it.hasNext())
					results.add(new DoubleVector(m_wrapper.getVotesForInstance( it.next() )));
					
				objs.add(results);
				//objs.add( new DoubleVector(new double[]{0,0}));
				objs.add( 1 );
				objs.add(System.currentTimeMillis());
				m_collector.emit(tuple,objs);

				
			}
		}
		m_collector.ack(tuple);		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("instance_id", "instance", "prediction","votes", "timestamp"));
	}
	
}
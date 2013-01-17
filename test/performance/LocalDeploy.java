package performance;

import java.util.List;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Random;

import javax.xml.bind.DatatypeConverter;

import moa.storm.topology.BaggingAggregator;
import moa.storm.topology.BenchmarkingTopology;
import moa.storm.topology.EchoFunction;
import moa.storm.topology.StormClusterTopology;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.fluent.GroupedStream;
import storm.trident.operation.Aggregator;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.ReadOnlyState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.ReadOnlyMapState;
import storm.trident.tuple.TridentTuple;
import weka.core.Instance;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

public class LocalDeploy {
    public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {{
        put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
        put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
        put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
     }};
     
     public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {{
         put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
         put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
         put("tim", Arrays.asList("alex"));
         put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
         put("adam", Arrays.asList("david", "carissa"));
         put("mike", Arrays.asList("john", "bob"));
         put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
     }};
     
     private static final class AggregatorTest implements
			Aggregator<Long> {
  		static int count = 0;
    	int instance;

    	
		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			instance = count++;
    		System.out.println("::::: created aggregator"+ instance);
			
		}

		@Override
		public void cleanup() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Long init(Object batchId, TridentCollector collector) {
			// TODO Auto-generated method stub
			return new Long(0);
		}

		@Override
		public void aggregate(Long val, TridentTuple tuple,
				TridentCollector collector) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void complete(Long val, TridentCollector collector) {
			// TODO Auto-generated method stub
			
		}
    	 
	}

	private static final class PrintFunction extends BaseFunction {
    	 
    	 
 		static int count = 0;
    	int instance;
 
		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			// TODO Auto-generated method stub
			super.prepare(conf, context);
    		instance = count++;
    		System.out.println("::::: created printer"+ instance);
			
		}

		@Override
		public void execute(TridentTuple arg0, TridentCollector arg1) {
			System.out.println(arg0);
			
		}
	}

	private static final class TestQueryFunction extends
			BaseQueryFunction<State, Long> {
    	 
    	@Override
		public void prepare(Map conf, TridentOperationContext context) {
			// TODO Auto-generated method stub
			super.prepare(conf, context);
    		instance = count++;
    		System.out.println("::::: created "+ instance);
			
		}

		static int count = 0;
    	int instance;
    	
    	
		@Override
		public List<Long> batchRetrieve(State arg0,
				List<TridentTuple> arg1) {
			int i = 0;
			List<Long> l;
			l = new ArrayList<Long>();
			for (TridentTuple t : arg1)
				l.add(new Long(i++));
			return l;
		}

		@Override
		public void execute(TridentTuple tuple, Long retrieved,
				TridentCollector collector) {
			System.out.println("::::: using "+ instance);
			Object value = tuple.getValue(0);
			Object serializedObject = null;
			if ( value instanceof String )
			{
				byte[] b = DatatypeConverter.parseBase64Binary(String.valueOf(value));
		        ObjectInputStream is;
		        
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
			}
			else
			if (value instanceof Instance)
			{
				serializedObject =value;
			}

			List<Object> objs = new ArrayList<Object>();
			objs.add( retrieved);
			objs.add( new Random().nextInt());
			objs.add( value );
			collector.emit(objs);

		}
	}

	public static class StaticSingleKeyMapState extends ReadOnlyState implements ReadOnlyMapState<Object> {
         public static class Factory implements StateFactory {
             Map _map;
             
             public Factory(Map map) {
                 _map = map;
             }

			@Override
			public State makeState(Map arg0, int arg1, int arg2) {
				return new StaticSingleKeyMapState(_map);
			}
             
         }
         
         Map _map;
         
         public StaticSingleKeyMapState(Map map) {
             _map = map;
         }
         
         
         @Override
         public List<Object> multiGet(List<List<Object>> keys) {
             List<Object> ret = new ArrayList();
             for(List<Object> key: keys) {
                 Object singleKey = key.get(0);
                 ret.add(_map.get(singleKey));
             }
             return ret;
         }
         
     }
	
	public static void main(String[] args) throws IOException
	{
		LocalCluster local = new LocalCluster();
		TridentTopology topology = new TridentTopology();
		Config conf = new Config();
		conf.setMaxSpoutPending(1);
		conf.put("topology.spout.max.batch.size", 2);
		BenchmarkingTopology storm = new BenchmarkingTopology("/ml_storm_cluster.properties");
		Stream s = storm.createLearningStream(null, topology).parallelismHint(2);
		TridentState[] state =  new TridentState[2];
		state[0] = topology.newStaticState(  new StaticSingleKeyMapState.Factory(FOLLOWERS_DB));
		state[1] = topology.newStaticState(  new StaticSingleKeyMapState.Factory(FOLLOWERS_DB));
		Stream[] streams= new Stream[2];
		s = s.name("source");
		for (int i = 0 ;i < 2 ; i ++)
		{
		    Stream substream = s.each(new Fields("instance"), new EchoFunction(i), new Fields()).parallelismHint(20);//.name("substream "+ i).groupBy(new Fields("instance")).name("grouped stream " +i);
			streams[i]= substream.stateQuery(state[i], new Fields("instance"),new TestQueryFunction() , new Fields("addition", "random"));//.parallelismHint(2).name("state query "+i);
		}
		
		
		topology.merge(streams).groupBy(new Fields("instance")).partitionAggregate(new Fields("addition","random", "instance"), new AggregatorTest(),  new Fields("test"));
		
		//each(new Fields("addition", "random","instance"),new PrintFunction(), new Fields("output"));
		conf.setNumWorkers(256);
		StormTopology tp = topology.build();
		local.submitTopology("topology", conf, tp);
	}
}

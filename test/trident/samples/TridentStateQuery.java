package trident.samples;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import performance.TopologyPrinter;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.QueryFunction;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.ValueUpdater;
import storm.trident.state.snapshot.Snapshottable;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.task.IMetricsContext;
import backtype.storm.testing.AckTracker;
import backtype.storm.testing.FeederSpout;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.TestJob;
import backtype.storm.testing.TrackedTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/** 
 * Demonstrates Trident State Quering
 *
 */
public class TridentStateQuery implements Serializable{
	
	 private static final class LocalTestJob implements TestJob, Serializable {
		@Override
		public void run(ILocalCluster cluster) throws Exception {
			TridentTopology topology = new TridentTopology();
			
			AckTracker tracker = new AckTracker(); 
			FeederSpout feeder = new FeederSpout(new Fields("num"));
			feeder.setAckFailDelegate(tracker);
			
			FeederSpout update = new FeederSpout(new Fields("num"));
			update.setAckFailDelegate(tracker);
			
			HashMap<String,String> map = new HashMap<String,String>();
			map.put("a","b");
			
			StaticSingleKeyMapState.Factory keyStateFactory = new StaticSingleKeyMapState.Factory(map);
			TridentState state = topology.newStaticState(keyStateFactory);
			
			Stream updateStream = topology.newStream("update", update );
			updateStream.persistentAggregate(keyStateFactory, 
					new CombinerAggregator<String>()
					{

						@Override
						public String init(TridentTuple tuple) {
							// TODO Auto-generated method stub
							return "1";
						}

						@Override
						public String combine(String val1, String val2) {
							// TODO Auto-generated method stub
							return val1+val2;
						}

						@Override
						public String zero() {
							// TODO Auto-generated method stub
							return "";
						}
						
					}
					
			, new Fields("vals"));
			
			Stream stream = topology.newStream("stream", feeder);
			stream.stateQuery(state, new QueryFunction<StaticSingleKeyMapState, String>() {

				@Override
				public void prepare(Map conf,
						TridentOperationContext context) {
					// TODO Auto-generated method stub
					
				}

				@Override
				public void cleanup() {
					// TODO Auto-generated method stub
					
				}

				@Override
				public List<String> batchRetrieve(StaticSingleKeyMapState state,
						List<TridentTuple> args) {
					System.out.println(state);
					ArrayList<String> ret = new ArrayList<String>();
					for (int i = 0 ; i < args.size(); i ++ ) 
					{
						ret.add("1");
					}
					return ret;
				}

				@Override
				public void execute(TridentTuple tuple, String result,
						TridentCollector collector) {
					ArrayList<Object> ret = new ArrayList<Object>();
					ret.add("a");
					collector.emit(ret);
					
				}
			}, new Fields("message") );
			
			TopologyPrinter printer = new TopologyPrinter();
			
			try {
				printer.print(topology.build());
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			TrackedTopology tracked = Testing.mkTrackedTopology(cluster,topology.build());
			
			Config conf = new Config();
			conf.setNumAckers(1);
			conf.setNumWorkers(1);
			conf.setMaxSpoutPending(1);
			conf.put("topology.spout.max.batch.size", 1);
			cluster.submitTopology("test",conf, tracked.getTopology());
			update.feed(new Values("A"));
			update.feed(new Values("B"));
			feeder.feed(new Values("X"));
			feeder.feed(new Values("C"));
			Testing.trackedWait(tracked, 4);
			tracker.resetNumAcks();
		}
	}

	public static class StaticSingleKeyMapState implements Snapshottable<Object>,State {
	        public static class Factory implements StateFactory {
	            Map _map;

	            public Factory(Map map) {
	                _map = map;
	            }

	            @Override
	            public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
	                return new StaticSingleKeyMapState(_map);
	            }

	        }

	        Map _map;

	        public StaticSingleKeyMapState(Map map) {
	            _map = map;
	        }


	     
	        public List<Object> multiGet(List<List<Object>> keys) {
	            List<Object> ret = new ArrayList();
	            for(List<Object> key: keys) {
	                Object singleKey = key.get(0);
	                ret.add(_map.get(singleKey));
	            }
	            return ret;
	        }


		
			public List<Object> multiUpdate(List<List<Object>> keys,
					List<ValueUpdater> updaters) {
				return multiGet(keys);
			}


			
			public void multiPut(List<List<Object>> keys, List<Object> vals) {
				// TODO Auto-generated method stub
				
			}


			
			public void beginCommit(Long txid) {
				// TODO Auto-generated method stub
				
			}


			
			public void commit(Long txid) {
				// TODO Auto-generated method stub
				
			}



			@Override
			public Object get() {
				return something;
			}



			@Override
			public Object update(ValueUpdater updater) {
				return updater.update(something);
			}



			@Override
			public void set(Object o) {
					something = o;
			}

			private Object something;
		

	    }

	public static void main(String[] args){
	
		MkClusterParam mkClusterParam = new MkClusterParam();
		mkClusterParam.setSupervisors(1);
		mkClusterParam.setPortsPerSupervisor(1);

		
		Testing.withTrackedCluster(mkClusterParam, new LocalTestJob());
	}
}

package moa.trident.topology;

import java.util.ArrayList;
import java.util.Map;

import moa.classifiers.Classifier;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.Function;
import storm.trident.state.StateFactory;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;


/** 
 * Standard topology for learning and evaluating moa classifier
 * @author bsp
 *
 */
public abstract class LearnEvaluateTopology {
	

	
	
	private static final String FIELD_STATISTICS = "statistics";
	public static final String FIELD_PREDICTION = "prediction";
	public static final String FIELD_TUPLE_ID = "tuple_id";
	public static final String FIELD_ID = "id";
	public static final String FIELD_INSTANCE = "instance";
	public static final String RPC_STATS = "stats";
	public static final String RPC_EVALUATE = "evaluate";
	public static final String RPC_CLASSIFIER = "classifier";
	
	public static final String FIELD_CLASSIFIER = "classifier";
	public static final String BAGGING_ENSEMBLE_SIZE = "bagging.ensemble_size";
	private static final String FIELD_WEIGHT = "weight";
	private static final String COMBINED_PREDICTION = "combined_prediction";
	
	
	public TridentTopology createOzaBagLearner(Map options)
	{
		TridentTopology topology = new TridentTopology();
		
		LocalDRPC drpc = getDRPC(options);
		 
		
		int ensemble_size = Integer.parseInt(String.valueOf(options.get(BAGGING_ENSEMBLE_SIZE)));

		
		int par_learning = 1;
		String learning_parallelism = (String)options.get("learning.parallelism");
		if (learning_parallelism!= null)
			par_learning = Integer.parseInt(learning_parallelism);
 
		Deserialize deserialize = new Deserialize();
		deserialize.setEnsembleSize( ensemble_size );
		
		Stream learningStream = createLearningStream(options, topology).parallelismHint(par_learning).
				each(new Fields(FIELD_INSTANCE),deserialize, new Fields());
		
		ArrayList<Stream> toMerge = new ArrayList<Stream>();
		for (int i = 0 ; i < ensemble_size ; i ++ )
		{
			toMerge.add( learningStream.each(new Fields(FIELD_INSTANCE), new EchoFunction(i), new Fields(FIELD_ID)));
		}
		learningStream =topology.merge(toMerge);
		TridentState ensemble = learningStream.groupBy(new Fields(FIELD_ID)).
				persistentAggregate(createFactory("key_classifier"), new Fields(FIELD_ID,FIELD_INSTANCE),
				new BaggingLearnerAggregator(new LearnerWrapper( getClassifier(options) )), new Fields()).parallelismHint(ensemble_size);
		
		
		
		deserialize = new Deserialize();
		deserialize.setEnsembleSize( ensemble_size );

		
		int par_stream = 1;
		String stream_parallelism = (String)options.get("evaluation_stream.parallelism");
		if (stream_parallelism!= null)
			par_stream = Integer.parseInt(stream_parallelism);
		
		int par_merge = 4;
		String merge_parallelism = (String)options.get("evaluation_merge.parallelism");
		if (merge_parallelism!= null)
			par_merge = Integer.parseInt(stream_parallelism);
		

		Stream predictionStream = createPredictionStream(options, topology).parallelismHint(par_stream).
				each(new Fields(FIELD_INSTANCE,FIELD_TUPLE_ID),deserialize, new Fields());
		
		toMerge = new ArrayList<Stream>();
		for (int i = 0 ; i < ensemble_size ; i ++ )
		{
			toMerge.add( predictionStream.each(new Fields(FIELD_INSTANCE,FIELD_TUPLE_ID), new EchoFunction(i), new Fields(FIELD_ID)));
		}
		predictionStream =topology.merge(toMerge);
		predictionStream.groupBy(new Fields(FIELD_ID)).stateQuery(ensemble,new Fields(FIELD_ID,FIELD_INSTANCE,FIELD_TUPLE_ID), 
					new EvaluateQueryFunction(), new Fields(FIELD_PREDICTION)).parallelismHint(ensemble_size).
					//groupBy(new Fields(FIELD_TUPLE_ID)).
					//aggregate(new Fields(FIELD_PREDICTION,FIELD_ID,FIELD_TUPLE_ID,FIELD_INSTANCE),new BaggingCombiner(),new Fields(COMBINED_PREDICTION)).
					//each(new Fields(COMBINED_PREDICTION), outputQueue(options), new Fields());
					aggregate(new Fields(FIELD_PREDICTION,FIELD_ID,FIELD_TUPLE_ID,FIELD_INSTANCE),new BaggingAggregator(),new Fields(FIELD_INSTANCE,FIELD_PREDICTION)).
					each(new Fields(FIELD_PREDICTION,FIELD_INSTANCE), outputQueue(options), new Fields());

		return topology;

	}


	public TridentTopology create(Map options)
	{
		TridentTopology topology = new TridentTopology();
		
		LocalDRPC drpc = getDRPC(options);
		; 
		LearnerWrapper wrapper = new LearnerWrapper( getClassifier(options) );
		
		int par_learning = 1;
		String learning_parallelism = (String)options.get("learning.parallelism");
		if (learning_parallelism!= null)
			par_learning = Integer.parseInt(learning_parallelism);
		
		Stream learningStream = createLearningStream(options, topology).parallelismHint(par_learning);
		TridentState classifierState = learningStream.persistentAggregate(createFactory(System.currentTimeMillis()+""), new Fields(FIELD_INSTANCE), 
				new LearnerAggregator(wrapper), new Fields(FIELD_CLASSIFIER));
		
		//topology.newDRPCStream(RPC_CLASSIFIER, drpc).
			//	stateQuery(classifierState, new ClassifierQueryFunction(), new Fields(FIELD_CLASSIFIER));
		
		//topology.newDRPCStream(RPC_EVALUATE, drpc).
				//stateQuery(classifierState,new Fields("args"), new EvaluateQueryFunction(), new Fields(FIELD_PREDICTION, FIELD_INSTANCE));
		
		//topology.newDRPCStream(RPC_STATS, drpc).
			//	stateQuery(classifierState,  new StatQueryFunction(), new Fields(FIELD_STATISTICS));
		
		int par_evaluation = 1;
		String evaluation_parallelism = (String)options.get("evaluation.parallelism");
		if (evaluation_parallelism!= null)
			par_evaluation = Integer.parseInt(evaluation_parallelism);
		createPredictionStream(options, topology).parallelismHint(par_evaluation)
			.stateQuery(classifierState,new Fields(FIELD_INSTANCE), new EvaluateQueryFunction(), new Fields(FIELD_PREDICTION)).
			each(new Fields(FIELD_PREDICTION, FIELD_INSTANCE), outputQueue(options), new Fields()); 
		
		return topology;
	}
	
	


	public abstract Function outputQueue(Map options);

	public abstract Stream createPredictionStream(Map options, TridentTopology topology);
	public abstract Stream createLearningStream(Map options, TridentTopology topology);

	public abstract LocalDRPC getDRPC(Map options);

	public abstract StateFactory createFactory(String string);
	
	public abstract Classifier getClassifier(Map options);
	
	

}

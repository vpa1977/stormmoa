package moa.storm.tasks;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import moa.classifiers.Classifier;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.snapshot.ReadOnlySnapshottable;
import storm.trident.tuple.TridentTuple;
import weka.core.Instance;

public class EvaluateQueryFunction extends BaseQueryFunction<ReadOnlySnapshottable<LearnerWrapper>, LearnerWrapper> implements Serializable
{
	private static final long serialVersionUID = 1L;

	public void execute(TridentTuple tuple, LearnerWrapper result,
			TridentCollector collector) {
		Object value = tuple.getValue(1);
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

		if (result != null)
		{
			List<Object> objs = new ArrayList<Object>();
			objs.add( result.getClassifier().getVotesForInstance( (Instance)serializedObject));
			objs.add( tuple.getValue(0));
			objs.add( value );
			collector.emit(objs);
		}


		
	}

	public List<LearnerWrapper> batchRetrieve(ReadOnlySnapshottable<LearnerWrapper> state,
			List<TridentTuple> args) {
		LearnerWrapper theClassifier = state.get();
		ArrayList<LearnerWrapper> list = new ArrayList<LearnerWrapper>();
		Integer val = new Integer(0);
		for (TridentTuple t : args)
		{
			list.add(theClassifier);
		}
		return list;
	}

	
}
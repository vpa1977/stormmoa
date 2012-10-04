package moa.storm.tasks;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import moa.classifiers.Classifier;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.snapshot.ReadOnlySnapshottable;
import storm.trident.tuple.TridentTuple;

public class ClassifierQueryFunction extends BaseQueryFunction<ReadOnlySnapshottable<LearnerWrapper>, LearnerWrapper> implements Serializable
{
	private static final long serialVersionUID = 1L;

	public void execute(TridentTuple tuple, LearnerWrapper result,
			TridentCollector collector) {
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(bos);
			if (result != null) {
			os.writeObject(result.getClassifier());
			byte[] bytes = bos.toByteArray();
			String tmp =DatatypeConverter.printBase64Binary(bytes);
			ArrayList<Object> out = new ArrayList<Object>();
			out.add(tmp);
			collector.emit(out);
			}
			else
			{
				ArrayList<Object> out = new ArrayList<Object>();
				out.add("");
				collector.emit(out);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public List<LearnerWrapper> batchRetrieve(ReadOnlySnapshottable<LearnerWrapper> state,
			List<TridentTuple> args) {
		LearnerWrapper theClassifier = state.get();
		
		ArrayList<LearnerWrapper> list = new ArrayList<LearnerWrapper>();
		for (TridentTuple t : args)
		{
			list.add( theClassifier);
		}
		return list;
	}
	
}
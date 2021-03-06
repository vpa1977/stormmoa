package moa.trident.topology;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;

import javax.xml.bind.DatatypeConverter;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import weka.core.Instance;

/** 
 * Deserializes instance and assigns weight
 * @author bsp
 *
 */
public class Deserialize extends BaseFunction implements Function {


	private int m_ensemble_size;
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Object value = tuple.getValue(0);
		Instance inst = null;
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
			inst = (Instance)serializedObject;
		}
		else if (value instanceof Instance)	{
			inst = (Instance)value;
		} else{
			throw new RuntimeException("Cannot deserialize "+ value);
		}
		
		ArrayList<Object> output = new ArrayList<Object>();
		output.add(inst);
		collector.emit(output);
	}

	public void setEnsembleSize(int ensemble_size) {
		m_ensemble_size = ensemble_size;
	}

}

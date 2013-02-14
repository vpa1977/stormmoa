package moa.storm.kryo;

import java.io.ByteArrayOutputStream;

import moa.streams.generators.RandomTreeGenerator;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.ProtectedProperties;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class DenseInstanceSerializer  extends Serializer<DenseInstance> {

	class Accessor extends DenseInstance
	{
		public Accessor(DenseInstance parent)
		{
			super(parent);
		}
		
		public double[] getValues()
		{
			return m_AttValues;
		}
		public double getWeight()
		{
			return m_Weight;
		}
	}
	@Override
	public void write(Kryo kryo, Output output, DenseInstance object) {
		Accessor accesor = new Accessor(object);
		
		output.writeDouble(accesor.getWeight());
		kryo.writeObjectOrNull(output, accesor.getValues(),double[].class);
		//kryo.writeObjectOrNull(output, object.dataset(), Instances.class);
	}	

	@Override
	public DenseInstance read(Kryo kryo, Input input, Class<DenseInstance> type) {
		
		double weight = input.readDouble();
		double[] values =kryo.readObjectOrNull(input, double[].class);
		//Instances header = kryo.readObjectOrNull(input, Instances.class) ;
		DenseInstance newInstance = new DenseInstance(weight, values);
		//newInstance.setDataset(header);
		return newInstance;
	}
	

	
	public static void main(String[] test)
	{
		RandomTreeGenerator generator = new RandomTreeGenerator();
		generator.prepareForUse();
		Kryo k = new Kryo();
		Instance inst = (Instance) generator.nextInstance().copy();
		k.addDefaultSerializer(DenseInstance.class, DenseInstanceSerializer.class);
		k.addDefaultSerializer(Attribute.class, AttributeSerializer.class);
		k.addDefaultSerializer(Instances.class, InstancesSerializer.class);
		k.addDefaultSerializer(ProtectedProperties.class, ProtectedPropertiesSerializer.class);
		inst.setDataset(null);
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		Output out = new Output(bos);
		k.writeObject(out, inst);
		out.close();
		
		Input in = new Input( bos.toByteArray());
		inst = k.readObject(in, DenseInstance.class);
		
	}

}

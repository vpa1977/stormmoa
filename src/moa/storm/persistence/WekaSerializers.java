package moa.storm.persistence;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.esotericsoftware.kryo.serializers.FieldSerializer;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instances;
import weka.core.ProtectedProperties;
import backtype.storm.Config;

public class WekaSerializers {
	public static void register( Config conf)
	{
		conf.registerSerialization(DenseInstance.class, DenseInstanceSerializer.class);
		conf.registerSerialization(Attribute.class, AttributeSerializer.class);
		conf.registerSerialization(Instances.class, InstancesSerializer.class);
		conf.registerSerialization(ProtectedProperties.class, ProtectedPropertiesSerializer.class);
		conf.registerSerialization(SimpleDateFormat.class, SimpleDateFormatSerializer.class);
	}
}

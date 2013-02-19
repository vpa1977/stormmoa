package moa.storm.kryo;

import weka.core.Attribute;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

public class AttributeSerializer extends FieldSerializer<Attribute> {

	@Override
	protected Attribute createCopy(Kryo kryo, Attribute original) {
		return (Attribute) original.copy();
	}

	@Override
	protected Attribute create(Kryo kryo, Input input, Class<Attribute> type) {
		return new Attribute("<empty>");
	}

	public AttributeSerializer(Kryo kryo, Class type) {
		super(kryo, type);
		getField("m_DateFormat").setCanBeNull(true);
		getField("m_Hashtable").setCanBeNull(true);
		getField("m_Values").setCanBeNull(true);
		getField("m_Header").setCanBeNull(true);
		getField("m_Metadata").setCanBeNull(true);
	}
	
}

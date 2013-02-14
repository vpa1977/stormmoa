package moa.storm.kryo;

import java.text.SimpleDateFormat;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

public class SimpleDateFormatSerializer extends FieldSerializer<SimpleDateFormat> {

	public SimpleDateFormatSerializer(Kryo kryo, Class type) {
		super(kryo, type);
		// TODO Auto-generated constructor stub
	}


}

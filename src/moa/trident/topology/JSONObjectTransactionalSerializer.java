package moa.trident.topology;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.json.simple.JSONValue;

import storm.trident.state.Serializer;
import storm.trident.state.TransactionalValue;

public class JSONObjectTransactionalSerializer implements Serializer<TransactionalValue> {
    @Override
    public byte[] serialize(TransactionalValue obj) {
        List toSer = new ArrayList(2);
        toSer.add(obj.getTxid());
        Object value = obj.getVal();

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream( bos );
            oos.writeObject(value);
            toSer.add( DatatypeConverter.printBase64Binary(bos.toByteArray()));
            return JSONValue.toJSONString(toSer).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
        	throw new RuntimeException(e);		}
    }

    @Override
    public TransactionalValue deserialize(byte[] b) {
        try {
            String s = new String(b, "UTF-8");
            List deser = (List) JSONValue.parse(s);
            byte[] bb = (byte[])DatatypeConverter.parseBase64Binary(String.valueOf(deser.get(1)));
            ObjectInputStream ois = new ObjectInputStream( new ByteArrayInputStream(bb));
            return new TransactionalValue((Long) deser.get(0), ois.readObject());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
        	throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
        	throw new RuntimeException(e);		}
    }
    
}


package br.com.lucasaquiles.kafkaavro.serializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

@Slf4j
public class AvroSerializer<T extends SpecificRecordBase> implements Serializer<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, T data) {

        byte[] result = null;

        try{

            log.info("M=serializer, data={}", data);

            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            BinaryEncoder binaryEncoder =
                    EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);

            DatumWriter<T> datumWriter = new GenericDatumWriter<>(data.getSchema());
            datumWriter.write(data, binaryEncoder);

            binaryEncoder.flush();
            byteArrayOutputStream.close();

            result = byteArrayOutputStream.toByteArray();

            log.info("M=serializer, serialized data={}", result);

            return result;
        }catch(IOException ex){

            throw new SerializationException("Can't serialize data "+data+" for topic "+topic );
        }
    }

    @Override
    public void close() {

    }
}

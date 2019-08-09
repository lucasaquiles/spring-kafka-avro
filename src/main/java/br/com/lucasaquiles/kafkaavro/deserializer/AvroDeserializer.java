package br.com.lucasaquiles.kafkaavro.deserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Arrays;
import java.util.Map;

@Slf4j
public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {
    protected final Class<T> targetType;

    public AvroDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public void close() {
        // No-op
    }

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
        // No-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            T result = null;

            if (data != null) {
                log.debug("data='{}'", data  );

                DatumReader<GenericRecord> datumReader =
                        new SpecificDatumReader<>(targetType.newInstance().getSchema());
                Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);

                result = (T) datumReader.read(null, decoder);
                log.debug("deserialized data='{}'", result);
            }
            return result;
        } catch (Exception ex) {
            throw new SerializationException(
                    "Can't deserialize data '" + Arrays.toString(data) + "' from topic '" + topic + "'", ex);
        }
    }

}

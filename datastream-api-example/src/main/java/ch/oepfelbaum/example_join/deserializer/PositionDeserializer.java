package ch.oepfelbaum.example_join.deserializer;

import ch.oepfelbaum.example_join.Utils;
import ch.oepfelbaum.example_join.model.Position;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class PositionDeserializer implements DeserializationSchema<Position> {
    @Override
    public Position deserialize(byte[] bytes) throws IOException {
        return Utils.getMapper().readValue(bytes, Position.class);
    }

    @Override
    public boolean isEndOfStream(Position nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Position> getProducedType() {
        return TypeInformation.of(Position.class);
    }
}

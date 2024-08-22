package ch.oepfelbaum.example_join.deserializer;

import ch.oepfelbaum.example_join.Utils;
import ch.oepfelbaum.example_join.model.Asset;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class AssetDeserializer implements DeserializationSchema<Asset> {
    @Override
    public Asset deserialize(byte[] bytes) throws IOException {
        return Utils.getMapper().readValue(bytes, Asset.class);
    }

    @Override
    public boolean isEndOfStream(Asset nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Asset> getProducedType() {
        return TypeInformation.of(Asset.class);
    }
}

package ch.oepfelbaum.example_join;

import ch.oepfelbaum.example_join.model.Asset;
import ch.oepfelbaum.example_join.model.Position;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class PositionWithAssetJoinFunction extends KeyedCoProcessFunction<String, Asset, Position, Position> {

    ValueState<Boolean> isAssetPresentState;
    ValueState<Position> positionState;

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Boolean> assetDescriptor =
                new ValueStateDescriptor<>(
                        "asset",
                        TypeInformation.of(Boolean.class),
                        null);
        isAssetPresentState = getRuntimeContext().getState(assetDescriptor);

        ValueStateDescriptor<Position> positionDescriptor =
                new ValueStateDescriptor<>(
                        "position", // the state name
                        TypeInformation.of(Position.class),
                        null);
        positionState = getRuntimeContext().getState(positionDescriptor);
    }

    @Override
    public void processElement1(final Asset currentAsset,
            final KeyedCoProcessFunction<String, Asset, Position, Position>.Context ctx,
            final Collector<Position> out) throws IOException {
        final Optional<Boolean> optionalIsAssetPresent = Optional.ofNullable(this.isAssetPresentState.value());

        if (optionalIsAssetPresent.isPresent()) {
            return;
        }

        this.isAssetPresentState.update(true);

        final Optional<Position> optionalPosition = Optional.ofNullable(positionState.value());
        optionalPosition.ifPresent(out::collect);
    }

    @Override
    public void processElement2(final Position currentPosition,
            final KeyedCoProcessFunction<String, Asset, Position, Position>.Context ctx,
            final Collector<Position> out)
            throws Exception {

        final Position oldValue = positionState.value();
        if (Objects.nonNull(oldValue) && oldValue.getTimestamp() > currentPosition.getTimestamp()) {
            return;
        }

        positionState.update(currentPosition);

        // Check for asset
        final Optional<Boolean> isAssetPresentOptional = Optional.ofNullable(isAssetPresentState.value());
        if (isAssetPresentOptional.isPresent() && isAssetPresentOptional.get()) {
            out.collect(currentPosition);
        }
    }
}

package master2018.flink.mapfunction;

import master2018.flink.datatypes.PositionEvent;
import master2018.flink.datatypes.SpeedFine;
import org.apache.flink.api.common.functions.MapFunction;

public class ToSpeedFine implements MapFunction<PositionEvent, SpeedFine> {
    @Override
    public SpeedFine map(PositionEvent positionEvent) throws Exception {
        SpeedFine speedFine = new SpeedFine(positionEvent);
        return speedFine;
    }
}
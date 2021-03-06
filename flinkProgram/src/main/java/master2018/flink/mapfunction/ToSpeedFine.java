package master2018.flink.mapfunction;

import master2018.flink.datatypes.PositionEvent;
import master2018.flink.datatypes.SpeedFine;
import org.apache.flink.api.common.functions.MapFunction;

/**
 *  TOSPEEDFINE CLASS
 *
 *  This class is used to generate a fine starting from
 *  PositionEvent of a car once it was faster than 90 mph.
 */
public class ToSpeedFine implements MapFunction<PositionEvent, SpeedFine> {
    @Override
    public SpeedFine map(PositionEvent positionEvent) throws Exception {
        SpeedFine speedFine = new SpeedFine(positionEvent);
        return speedFine;
    }
}
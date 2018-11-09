package master2018.flink.operator;

import master2018.flink.datatypes.PositionEvent;
import master2018.flink.datatypes.SpeedFine;
import master2018.flink.mapfunction.ToSpeedFine;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 *  SPEEDRADAR CLASS
 *
 *  This class performs the job of finding those cars
 *  that exceed 90km/h.
 *
 *  Returns a single data stream of SpeedFine objects.
 */
public class SpeedRadar  {

    private static final int MAXIMUM_SPEED = 90;

    public SingleOutputStreamOperator<SpeedFine> run(DataStream<PositionEvent> stream) {
        return stream
                .filter((PositionEvent e) -> e.getSpeed() > MAXIMUM_SPEED)
                .map(new ToSpeedFine());
    }
}

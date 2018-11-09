package master2018.flink.operator;

import master2018.flink.datatypes.AvgSpeedFine;
import master2018.flink.datatypes.PositionEvent;
import master2018.flink.windowfunction.AvgSpeedWindow;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 *  AVGSPEEDCHECK CLASS
 *
 *  This class perform the job of finding those cars
 *  that drive faster than 60 mph on average between the highway segments 52 and 56, considering both directions.
 *
 *  The class creates a new single output stream of AvgSpeedFines.
 */

public class AvgSpeedCheck {

    private static final int SEGMENT_START = 52;
    private static final int SEGMENT_END = 56;
    private static final double SPEED_CONVERSION = 2.23694;
    private static final double MAXIMUM_SPEED = 60.0;

    public SingleOutputStreamOperator<AvgSpeedFine> run(DataStream<PositionEvent> stream) {
        return stream
                .filter((PositionEvent e) -> (e.getSegment() >= SEGMENT_START
                        && e.getSegment() <= SEGMENT_END))
                // key by VID, Highway and direction
                .keyBy(1,3,5)
                // if there is a missing event, we assume a new trip started, therefore Gap = 31 sec
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .apply(new AvgSpeedWindow(MAXIMUM_SPEED, SPEED_CONVERSION));
    }
}

package master2018.flink.operator;

import master2018.flink.datatypes.AvgSpeedFine;
import master2018.flink.datatypes.PositionEvent;
import master2018.flink.keyselector.VidKey;
import master2018.flink.windowfunction.AvgSpeedWindow;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AvgSpeedCheck {

    public static final int SEGMENT_START = 52;
    public static final int SEGMENT_END = 56;
    public static final double SPEED_CONVERSION = 2.23694;
    public static final double MAXIMUM_SPEED = 60.0;

    public SingleOutputStreamOperator<AvgSpeedFine> run(DataStream<PositionEvent> stream) {
        return stream
                .filter((PositionEvent e) -> (e.getSegment() >= SEGMENT_START
                        && e.getSegment() <= SEGMENT_END)).setParallelism(1)
                .keyBy(new VidKey())
                .window(EventTimeSessionWindows.withGap(Time.seconds(31))) // if there is a missing event, we assume a new trip started
                .apply(new AvgSpeedWindow(SEGMENT_START, SEGMENT_END, MAXIMUM_SPEED, SPEED_CONVERSION));
    }
}

package master2018.flink.operator;

import master2018.flink.datatypes.AvgSpeedFine;
import master2018.flink.datatypes.PositionEvent;
import master2018.flink.keyselector.VidKey;
import master2018.flink.windowfunction.AvgSpeedWindow;
import master2018.flink.windowfunction.AvgSpeedWindowTwo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
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
                        && e.getSegment() <= SEGMENT_END))//.setParallelism(1)
                //.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PositionEvent>() {
                //                @Override
                //                public long extractAscendingTimestamp(PositionEvent element) {
                //                    return element.getTime()*1000;
                //                }
                //            })
                .keyBy(1,3,5)//new VidKey())
                .window(EventTimeSessionWindows.withGap(Time.seconds(31))) // if there is a missing event, we assume a new trip started
                .apply(new AvgSpeedWindowTwo(SEGMENT_START, SEGMENT_END, MAXIMUM_SPEED, SPEED_CONVERSION));
    }
}

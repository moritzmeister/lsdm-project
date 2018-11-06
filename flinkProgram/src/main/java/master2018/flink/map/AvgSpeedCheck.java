package master2018.flink.map;

import master2018.flink.datatypes.AvgSpeedFine;
import master2018.flink.datatypes.PositionEvent;
import master2018.flink.keyselector.VidKey;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

public class AvgSpeedCheck {

    public static final int SEGMENT_START = 52;
    public static final int SEGMENT_END = 56;
    public static final double SPEED_CONVERSION = 2.23694;
    public static final double MAXIMUM_SPEED = 60.0;

    public SingleOutputStreamOperator<AvgSpeedFine> run(DataStream<PositionEvent> stream) {
        return stream
                .filter((PositionEvent e) -> (e.getSegment() >= SEGMENT_START
                        && e.getSegment() <= SEGMENT_END)).setParallelism(1)
/*
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PositionEvent>() {
                    @Override
                    public long extractAscendingTimestamp(PositionEvent positionEvent) {
                        return positionEvent.getTime() * 1000;
                    }
                })
*/
                .keyBy(new VidKey())
                .window(EventTimeSessionWindows.withGap(Time.seconds(31))) // if there is a missing event, we assume a new trip started
                .apply(new avgSpeedWindow());
    }

    private static class avgSpeedWindow implements WindowFunction<PositionEvent, AvgSpeedFine,
            Tuple3<String, Integer, Integer>, TimeWindow> {

        private AvgSpeedFine output = new AvgSpeedFine();

        @Override
        public void apply(Tuple3<String, Integer, Integer> key, TimeWindow  timeWindow,
                          Iterable<PositionEvent> iterable, Collector<AvgSpeedFine> collector) {

            Iterator<PositionEvent> events = iterable.iterator();

            PositionEvent currentElement;
            PositionEvent oldElement;
            PositionEvent firstElement;
            PositionEvent lastElement = null;

            double avgSpeed = 0;
            boolean allSegments = false;

            try {
                currentElement = events.next();
                firstElement = currentElement;
                // check if first element is from start segment, only then continue to loop through all events in window
                if (currentElement.getSegment() == SEGMENT_START
                        || currentElement.getSegment() == SEGMENT_END
                        ) {
                    allSegments = true;
                    System.out.println("start window");
                }

                while (events.hasNext() && allSegments) {
                    oldElement = currentElement;
                    currentElement = events.next();

                    // check if next element is from same segment or the one after
                    if ( !(currentElement.getSegment() == oldElement.getSegment()
                            || currentElement.getSegment() == (oldElement.getSegment()+1)
                            || currentElement.getSegment() == (oldElement.getSegment()-1)
                    ) ) {
                        allSegments = false;
                        System.out.println("unfinished window");
                    }

                    if (currentElement.getSegment() == SEGMENT_END && firstElement.getSegment() == SEGMENT_START) {
                        lastElement = currentElement;
                    }
                    else if (currentElement.getSegment() == SEGMENT_START && firstElement.getSegment() == SEGMENT_END) {
                        lastElement = currentElement;
                    }
                }

                if (allSegments) {
                    //switch first and last element if needed

                    //if (firstElement.getSegment() == SEGMENT_END) {
                    //    currentElement = firstElement;
                    //    firstElement = lastElement;
                    //    lastElement = currentElement;
                    //}

                    // calc speed
                    avgSpeed = ((Math.abs(lastElement.getPosition() - firstElement.getPosition()) * 1.0)
                            / (lastElement.getTime() - firstElement.getTime())) * SPEED_CONVERSION;

                    if (avgSpeed > MAXIMUM_SPEED) {
                        output.setTime1(firstElement.getTime());
                        output.setTime2(lastElement.getTime());
                        output.setVid(key.f0);
                        output.setXway(key.f1);
                        output.setDirection(key.f2);
                        output.setAvgSpeed(avgSpeed);
                        collector.collect(output);
                    }
                }
            } catch (Exception e) {

            }
        }
    }
}

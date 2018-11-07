package master2018.flink.windowfunction;

import master2018.flink.datatypes.AvgSpeedFine;
import master2018.flink.datatypes.PositionEvent;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class AvgSpeedWindow implements WindowFunction<PositionEvent, AvgSpeedFine,
        Tuple3<String, Integer, Integer>, TimeWindow> {

    private AvgSpeedFine output = new AvgSpeedFine();
    private int seg_start;
    private int seg_end;
    private double max_speed;
    private double conversion;

    public AvgSpeedWindow(int start, int end, double max, double conv) {
        this.seg_start = start;
        this.seg_end = end;
        this.max_speed = max;
        this.conversion = conv;
    }

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
            if (currentElement.getSegment() == seg_start
                    || currentElement.getSegment() == seg_end
                    ) {
                allSegments = true;
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
                }

                if (currentElement.getSegment() == seg_end && firstElement.getSegment() == seg_start) {
                    lastElement = currentElement;
                }
                else if (currentElement.getSegment() == seg_start && firstElement.getSegment() == seg_end) {
                    lastElement = currentElement;
                }
            }

            if (allSegments) {
                // calc speed
                avgSpeed = ((Math.abs(lastElement.getPosition() - firstElement.getPosition()) * 1.0)
                        / (lastElement.getTime() - firstElement.getTime())) * conversion;

                if (avgSpeed > max_speed) {
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
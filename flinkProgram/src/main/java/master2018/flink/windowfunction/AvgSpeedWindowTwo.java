package master2018.flink.windowfunction;

import master2018.flink.datatypes.AvgSpeedFine;
import master2018.flink.datatypes.PositionEvent;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class AvgSpeedWindowTwo implements WindowFunction<PositionEvent, AvgSpeedFine,
        Tuple, TimeWindow> {

    private int seg_start;
    private int seg_end;
    private double max_speed;
    private double conversion;

    public AvgSpeedWindowTwo(int start, int end, double max, double conv) {
        this.seg_start = start;
        this.seg_end = end;
        this.max_speed = max;
        this.conversion = conv;
    }

    @Override
    public void apply(Tuple key, TimeWindow timeWindow,
                      Iterable<PositionEvent> iterable, Collector<AvgSpeedFine> collector) {

        if (Iterables.size(iterable) < 5) return;

        PositionEvent firstElement = null;
        PositionEvent lastElement = null;

        HashSet<Integer> uniqueSegments = new HashSet<>();

        for (PositionEvent currentElement : iterable) {
            uniqueSegments.add(currentElement.getSegment());

            if (firstElement == null || currentElement.getTime() < firstElement.getTime()) {
                firstElement = currentElement;
            }
            if (lastElement == null || currentElement.getTime() > lastElement.getTime()) {
                lastElement = currentElement;
            }
        }

        if (uniqueSegments.size() == 5) {
            // calc speed
            double avgSpeed = ((Math.abs(lastElement.getPosition() - firstElement.getPosition()) * 1.0)
                    / (lastElement.getTime() - firstElement.getTime())) * conversion;

            if (avgSpeed > max_speed) {
                collector.collect(new AvgSpeedFine(firstElement, lastElement, key, avgSpeed));
            }
        }
    }
}

package master2018.flink.windowfunction;

import master2018.flink.datatypes.AvgSpeedFine;
import master2018.flink.datatypes.PositionEvent;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * AVGSPEEDWINDOW Class
 *
 * This class implements the functionality of finding the
 * average speed of a car between the segments 52 and 56, for both directions.
 *
 * The apply mehtod is invoked once flink closes the SessionEventTime window with Gap of 31 seconds.
 * That is when there are no more events for 31 seconds for a group of events, grouped by VID, Highway and Direction.
 *
 */

public class AvgSpeedWindow implements WindowFunction<PositionEvent, AvgSpeedFine,
        Tuple, TimeWindow> {

    private double max_speed;
    private double conversion;

    public AvgSpeedWindow(double max, double conv) {
        this.max_speed = max;
        this.conversion = conv;
    }

    @Override
    public void apply(Tuple key, TimeWindow timeWindow,
                      Iterable<PositionEvent> iterable, Collector<AvgSpeedFine> collector) {

        // since the car needs to pass through all segments between 52 and 56 (included), we don't need to consider
        // the window when it contains less than 5 events
        if (Iterables.size(iterable) < 5) return;

        PositionEvent firstElement = null;
        PositionEvent lastElement = null;

        // we use a hash set to keep track of the unique segments visited by the car in the window
        HashSet<Integer> uniqueSegments = new HashSet<>();

        // Iteration through all events in the window to check if all segments are present
        // and to find the first and last position in the interval
        for (PositionEvent currentElement : iterable) {
            uniqueSegments.add(currentElement.getSegment());

            // finding the first and last PositionEvent in the window
            if (firstElement == null || currentElement.getTime() < firstElement.getTime()) {
                firstElement = currentElement;
            }
            if (lastElement == null || currentElement.getTime() > lastElement.getTime()) {
                lastElement = currentElement;
            }
        }

        // Check if all segments were visited
        if (uniqueSegments.size() == 5) {
            // calc speed
            double avgSpeed = ((Math.abs(lastElement.getPosition() - firstElement.getPosition()) * 1.0)
                    / (lastElement.getTime() - firstElement.getTime())) * conversion;

            // Collect the new AvgSpeedFine event if the car was to fast.
            if (avgSpeed > max_speed) {
                collector.collect(new AvgSpeedFine(firstElement, lastElement, key, avgSpeed));
            }
        }
    }
}

package master2018.flink.windowfunction;

import master2018.flink.datatypes.Accident;
import master2018.flink.datatypes.PositionEvent;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 *  ACCIDENT WINDOWS CLASS
 *
 *  This class contains the window function to be executed when
 *  looking for an accident. If a car has an accident, It is
 *  collected.
 *
 *  An accident occurs when a car remain in the same position
 *  for four consecutive emitted Timestamps.
 */
public class AccidentWindow implements WindowFunction<PositionEvent, Accident, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple key, TimeWindow  timeWindow,
                      Iterable<PositionEvent> iterable, Collector<Accident> collector) throws Exception {

        // Enter with 4 events only
        if (Iterables.size(iterable) == 4) {

            PositionEvent firstElement = null;
            PositionEvent lastElement = null;

            HashSet<Long> uniqueTimestamps = new HashSet<>();

            for (PositionEvent currentElement : iterable) {
                // Looking for distinct values
                uniqueTimestamps.add(currentElement.getTime());

                // Order the elements
                if (firstElement == null || currentElement.getTime() < firstElement.getTime()) {
                    firstElement = currentElement;
                }
                if (lastElement == null || currentElement.getTime() > lastElement.getTime()) {
                    lastElement = currentElement;
                }
            }

            // Collect
            if (uniqueTimestamps.size() == 4 && ((lastElement.getTime() - firstElement.getTime()) == 90)) {
                collector.collect(new Accident(firstElement, lastElement));
            }
        }
    }
}
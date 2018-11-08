package master2018.flink.windowfunction;

import master2018.flink.datatypes.Accident;
import master2018.flink.datatypes.PositionEvent;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class AccidentWindow implements WindowFunction<PositionEvent, Accident,
        Tuple, TimeWindow> {

    @Override
    public void apply(Tuple key, TimeWindow  timeWindow,
                      Iterable<PositionEvent> iterable, Collector<Accident> collector) {

        if (Iterables.size(iterable) != 4) return;

        PositionEvent firstElement = null;
        PositionEvent lastElement = null;

        HashSet<Long> uniqueTimestamps = new HashSet<>();

        for (PositionEvent currentElement : iterable) {
            uniqueTimestamps.add(currentElement.getTime());

            if (firstElement == null || currentElement.getTime() < firstElement.getTime()) {
                firstElement = currentElement;
            }
            if (lastElement == null || currentElement.getTime() > lastElement.getTime()) {
                lastElement = currentElement;
            }
        }

        if (uniqueTimestamps.size() == 4 && ((lastElement.getTime() - firstElement.getTime()) == 90)) {
            collector.collect(new Accident(firstElement, lastElement));
        }
    }
}
package master2018.flink.windowfunction;

import master2018.flink.datatypes.Accident;
import master2018.flink.datatypes.PositionEvent;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class AccidentWindow implements WindowFunction<PositionEvent, Accident,
        Tuple3<String, Integer, Integer>, GlobalWindow> {

    @Override
    public void apply(Tuple3<String, Integer, Integer> key, GlobalWindow globalWindow,
                      Iterable<PositionEvent> iterable, Collector<Accident> collector) {

        Iterator<PositionEvent> events = iterable.iterator();

        PositionEvent currentElement;
        PositionEvent oldElement;
        PositionEvent firstElement;

        try {
            oldElement = firstElement = events.next();
            currentElement = events.next();

            int count = 2;
            while (events.hasNext() && count < 4
                    && (currentElement.getTime() - oldElement.getTime()) == 30
                    && (currentElement.getPosition() == oldElement.getPosition())) {
                count++;
                oldElement = currentElement;
                currentElement = events.next();

                if (count == 4) {
                    Accident accident =
                            new Accident(firstElement, currentElement);
                    collector.collect(accident);
                }
            }
        } catch (Exception e) {

        }


    }
}
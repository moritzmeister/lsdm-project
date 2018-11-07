package master2018.flink.windowfunction;

import master2018.flink.datatypes.Accident;
import master2018.flink.datatypes.PositionEvent;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class AccidentWindow implements WindowFunction<PositionEvent, Accident,
        Tuple3<String, Integer, Integer>, TimeWindow> {

    @Override
    public void apply(Tuple3<String, Integer, Integer> key, TimeWindow  timeWindow,
                      Iterable<PositionEvent> iterable, Collector<Accident> collector) {

        Iterator<PositionEvent> events = iterable.iterator();

        List<PositionEvent> list = new ArrayList<>();
        while (events.hasNext()) {
            insert(list, events.next());
        }

        for(int i = 0; i+3 < list.size() ; i++)
            if ((list.get(i+3).getTime() - list.get(i).getTime() == 90)) {
                Accident accident =
                        new Accident(list.get(i), list.get(i+3));
                collector.collect(accident);
            }
    }

    public void insert(List<PositionEvent> list, PositionEvent x){
        // loop through all elements
        for (int i = 0; i < list.size(); i++) {
            // if the element you are looking at is smaller than x,
            // go to the next element
            if (list.get(i).getTime() < x.getTime()) continue;
            // if the element equals x, return, because we don't add duplicates
            if (list.get(i).getTime() == x.getTime()) return;
            // otherwise, we have found the location to add x
            list.add(i, x);
            return;
        }
        // we looked through all of the elements, and they were all
        // smaller than x, so we add ax to the end of the list
        list.add(x);
    }
}
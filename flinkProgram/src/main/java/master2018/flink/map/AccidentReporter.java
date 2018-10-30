package master2018.flink.map;

import master2018.flink.datatypes.Accident;
import master2018.flink.datatypes.PositionEvent;
import master2018.flink.keyselector.VidKey;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class AccidentReporter {

    public static final int ACCIDENT_SPEED = 0;

    public static SingleOutputStreamOperator<Accident> run(DataStream<PositionEvent> stream) {
        return stream
                .filter((PositionEvent e) -> e.getSpeed() == ACCIDENT_SPEED).setParallelism(1)
                .keyBy(new VidKey())
                .countWindow(4, 1)
                .apply(new CustomWindow());
    }

    public static class CustomWindow implements WindowFunction<PositionEvent, Accident,
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
                        Accident accident = new Accident();
                        accident.setTime1(firstElement.getTime());
                        accident.setTime2(currentElement.getTime());
                        accident.setVid(currentElement.getVid());
                        accident.setXWay(currentElement.getXway());
                        accident.setSegment(currentElement.getSegment());
                        accident.setDirection(currentElement.getDirection());
                        accident.setPosition(currentElement.getPosition());
                        collector.collect(accident);
                    }
                }
            } catch (Exception e) {

            }


        }
    }

}

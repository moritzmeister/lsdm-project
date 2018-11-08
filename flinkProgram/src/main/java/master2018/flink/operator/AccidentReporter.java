package master2018.flink.operator;

import master2018.flink.datatypes.Accident;
import master2018.flink.datatypes.PositionEvent;
import master2018.flink.windowfunction.AccidentWindow;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 *  ACCIDENTREPORTER CLASS
 *
 *  This class perform the job of finding those cars
 *  stopped because an accident occurred.
 *
 *  An accident occurs when a car remain in the same
 *  position for four consecutive emitted Timestamps.
 *
 */
public class AccidentReporter {

    private static final int ACCIDENT_SPEED = 0;

    public SingleOutputStreamOperator<Accident> run(DataStream<PositionEvent> stream) {
        return stream
                .filter((PositionEvent e) -> e.getSpeed() == ACCIDENT_SPEED)
                .keyBy(1,3,5)
                .window(SlidingEventTimeWindows.of(Time.seconds(120),Time.seconds(30)))
                .apply(new AccidentWindow());
    }
}

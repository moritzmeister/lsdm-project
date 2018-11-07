package master2018.flink.operator;

import master2018.flink.datatypes.Accident;
import master2018.flink.datatypes.PositionEvent;
import master2018.flink.keyselector.VidKey;
import master2018.flink.windowfunction.AccidentWindow;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AccidentReporter {

    public static final int ACCIDENT_SPEED = 0;

    public static SingleOutputStreamOperator<Accident> run(DataStream<PositionEvent> stream) {
        return stream
                .filter((PositionEvent e) -> e.getSpeed() == ACCIDENT_SPEED)
                .keyBy(new VidKey())
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .apply(new AccidentWindow());
    }
}

package master2018.flink.operator;

import master2018.flink.datatypes.Accident;
import master2018.flink.datatypes.PositionEvent;
import master2018.flink.keyselector.VidKey;
import master2018.flink.windowfunction.AccidentWindow;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class AccidentReporter {

    public static final int ACCIDENT_SPEED = 0;

    public static SingleOutputStreamOperator<Accident> run(DataStream<PositionEvent> stream) {
        return stream
                .filter((PositionEvent e) -> e.getSpeed() == ACCIDENT_SPEED).setParallelism(1)
                .keyBy(new VidKey())
                .countWindow(4, 1)
                .apply(new AccidentWindow());
    }
}

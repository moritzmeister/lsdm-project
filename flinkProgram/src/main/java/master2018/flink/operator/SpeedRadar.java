package master2018.flink.operator;

import master2018.flink.datatypes.PositionEvent;
import master2018.flink.datatypes.SpeedFine;
import master2018.flink.mapfunction.ToSpeedFine;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class SpeedRadar  {

    public static final int MAXIMUM_SPEED = 90;

    public static SingleOutputStreamOperator<SpeedFine> run(DataStream<PositionEvent> stream) {
        return stream
                .filter((PositionEvent e) -> e.getSpeed() > MAXIMUM_SPEED).setParallelism(1)
                .map(new ToSpeedFine());
    }
}

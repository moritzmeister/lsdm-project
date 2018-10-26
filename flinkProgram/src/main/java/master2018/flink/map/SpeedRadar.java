package master2018.flink.map;

import master2018.flink.datatypes.PositionEvent;
import master2018.flink.datatypes.SpeedFine;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class SpeedRadar  {

    public static final int MAXIMUM_SPEED = 90;

    public static SingleOutputStreamOperator<SpeedFine> run(DataStream<PositionEvent> stream) {
        return stream
                .filter((PositionEvent e) -> e.getSpeed() > MAXIMUM_SPEED).setParallelism(1)
                .map(new toSpeedFine());
    }

    public static class toSpeedFine implements MapFunction<PositionEvent, SpeedFine> {
        @Override
        public SpeedFine map(PositionEvent positionEvent) throws Exception {
            SpeedFine speedFine = new SpeedFine(positionEvent);
            return speedFine;
        }
    }

}

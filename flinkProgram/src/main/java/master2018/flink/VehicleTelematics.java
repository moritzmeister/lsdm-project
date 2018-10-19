package master2018.flink;

import master2018.flink.datatypes.PositionEvent;
import master2018.flink.sources.PositionSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class VehicleTelematics {

    public static final String SPEED_RADAR_FILE = "speedfines.csv";
    public static final String AVG_SPEED_FILE = "avgspeedfines.csv";
    public static final String ACCIDENTS_FILE = "accidents.csv";

    public static void main(String[] args) throws Exception {

        //if (args.length < 2) {
        //    System.out.println("Usage: <input file> <output folder>");
        //    throw new Exception();
        //}

        //String inputFile = args[0];
        //String outputFolder = args[1];

        String inputFile = "data/sample.csv";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<PositionEvent> positionStream = env.addSource(new PositionSource(inputFile)).setParallelism(1);

        positionStream.print().setParallelism(2);

        env.execute("vehicle-telematics");

    }
}

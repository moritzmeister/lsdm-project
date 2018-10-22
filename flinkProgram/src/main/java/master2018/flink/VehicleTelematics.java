package master2018.flink;

import master2018.flink.datatypes.PositionEvent;
import master2018.flink.datatypes.SpeedFine;
import master2018.flink.map.SpeedRadar;
import master2018.flink.source.PositionSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class VehicleTelematics {

    public static final String SPEEDFINES = "speedfines.csv";
    //public static final String AVG_SPEED_FILE = "avgspeedfines.csv";
    //public static final String ACCIDENTS_FILE = "accidents.csv";

    public static void main(String[] args) throws Exception {

        //if (args.length < 2) {
        //    System.out.println("Usage: <input file> <output folder>");
        //    throw new Exception();
        //}

        //String inputFile = args[0];
        //String outputFolder = args[1];

        String inputFile = "data/traffic-3xways.csv";
        String outputFolder = "output";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<PositionEvent> positionStream = env.addSource(new PositionSource(inputFile));

        // Init transformations
        SpeedRadar speedControl = new SpeedRadar();

        // Run jobs
        DataStream<SpeedFine> OutputFines= speedControl.run(positionStream).setParallelism(1);

        // Write final streams to output files
        OutputFines.writeAsText(outputFolder + "/" + SPEEDFINES, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //OutputFines.print().setParallelism(1);

        env.execute("vehicle-telematics");

    }
}

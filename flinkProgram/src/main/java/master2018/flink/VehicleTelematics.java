package master2018.flink;

import master2018.flink.datatypes.Accident;
import master2018.flink.datatypes.AvgSpeedFine;
import master2018.flink.datatypes.PositionEvent;
import master2018.flink.datatypes.SpeedFine;
import master2018.flink.keyselector.VidKey;
import master2018.flink.map.AccidentReporter;
import master2018.flink.map.AvgSpeedCheck;
import master2018.flink.map.SpeedRadar;
import master2018.flink.source.PositionSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class VehicleTelematics {

    public static final String SPEEDFINES = "speedfines.csv";
    public static final String AVGSPEEDFINES = "avgspeedfines.csv";
    public static final String ACCIDENTS = "accidents.csv";

    public static void main(String[] args) throws Exception {


        if (args.length < 2) {
            System.out.println("Usage: <input file> <output folder>");
            throw new Exception();
        }

        String inputFile = args[0];
        String outputFolder = args[1];

        //String inputFile = "data/traffic-3xways.csv";
        //String outputFolder = "output";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);

        DataStream<PositionEvent> positionStream = env.addSource(new PositionSource(inputFile));

        // Init transformations
        SpeedRadar speedControl = new SpeedRadar();
        AccidentReporter accidentsChecker = new AccidentReporter();
        AvgSpeedCheck avgSpeedChecker = new AvgSpeedCheck();

        KeyedStream<PositionEvent, Tuple3<String, Integer, Integer>> test = positionStream.filter((PositionEvent e) -> (e.getSegment() >= 52
                && e.getSegment() <= 56)).setParallelism(1)

                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PositionEvent>() {
                    @Override
                    public long extractAscendingTimestamp(PositionEvent positionEvent) {
                        return positionEvent.getTime() * 1000;
                    }
                })

                .keyBy(new VidKey());

        test.print();

        // Run jobs
        DataStream<SpeedFine> OutputFines= speedControl.run(positionStream);
        DataStream<Accident> OutputAccidents = accidentsChecker.run(positionStream);
        DataStream<AvgSpeedFine> OutputAvgSpeedFines = avgSpeedChecker.run(positionStream);

        //OutputAvgSpeedFines.print();

        // Write final streams to output files
        OutputFines.writeAsText(outputFolder + "/" + SPEEDFINES, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        OutputAccidents.writeAsText(outputFolder + "/" + ACCIDENTS, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        OutputAvgSpeedFines.writeAsText(outputFolder + "/" + AVGSPEEDFINES, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("vehicle-telematics");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

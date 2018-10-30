package master2018.flink;

import master2018.flink.datatypes.Accident;
import master2018.flink.datatypes.PositionEvent;
import master2018.flink.datatypes.SpeedFine;
import master2018.flink.keyselector.VidKey;
import master2018.flink.map.AccidentReporter;
import master2018.flink.map.SpeedRadar;
import master2018.flink.source.PositionSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class VehicleTelematics {

    public static final String SPEEDFINES = "speedfines.csv";
    //public static final String AVG_SPEED_FILE = "avgspeedfines.csv";
    public static final String ACCIDENTS = "accidents.csv";

    public static void main(String[] args) throws Exception {


        if (args.length < 2) {
            System.out.println("Usage: <input file> <output folder>");
            throw new Exception();
        }

        String inputFile = args[0];
        String outputFolder = args[1];

        //System.out.println(args[0]);
        //System.out.println(args[1]);
        //System.out.println("Working Directory = " + System.getProperty("user.dir"));

        //String inputFile = "data/test_accidents.csv";
        //String outputFolder = "output";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(4);

        DataStream<PositionEvent> positionStream = env.addSource(new PositionSource(inputFile));

        // Init transformations
        SpeedRadar speedControl = new SpeedRadar();
        AccidentReporter accidentsChecker = new AccidentReporter();

        // Run jobs
        DataStream<SpeedFine> OutputFines= speedControl.run(positionStream);
        DataStream<Accident> OutputAccidents = accidentsChecker.run(positionStream);

        // Keyed Stream test
        // DataStream<PositionEvent> keyed = positionStream.keyBy(new VidKey());

        // Write final streams to output files
        OutputFines.writeAsText(outputFolder + "/" + SPEEDFINES, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        OutputAccidents.writeAsText(outputFolder + "/" + ACCIDENTS, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("vehicle-telematics");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

package master2018.flink;

import master2018.flink.datatypes.Accident;
import master2018.flink.datatypes.AvgSpeedFine;
import master2018.flink.datatypes.PositionEvent;
import master2018.flink.datatypes.SpeedFine;
import master2018.flink.operator.AccidentReporter;
import master2018.flink.operator.AvgSpeedCheck;
import master2018.flink.operator.SpeedRadar;
import master2018.flink.source.PositionSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * This is an implementation of the 'vehicle-telematics' assignment for the course on Large Scale Data Management at
 * Universidad Politecnica de Madrid.
 * This is the main class to be run, in order to produce the desired results for the three functionalities.
 * We only enforce parallelism for the Data Source in order to preserve the ordering of events
 * and for Data Sinks to make sure only one output file per task is written.
 *
 * @author  Gioele Bigini, Moritz Meister
 * @version 1.0
 *
 */

public class VehicleTelematics {

    private static final String SPEEDFINES = "speedfines.csv";
    private static final String AVGSPEEDFINES = "avgspeedfines.csv";
    private static final String ACCIDENTS = "accidents.csv";

    public static void main(String[] args) throws Exception {

        // Getting input and output paths from terminal
        if (args.length < 2) {
            System.out.println("Usage: <input file> <output folder>");
            throw new Exception();
        }

        String inputFile = args[0];
        String outputFolder = args[1];

        // Init the StreamExecutionEnvironment and EventTime setting to tell Flink the context
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Generate the DataStream from input file using the custom source function
        DataStream<PositionEvent> positionStream = env.addSource(new PositionSource(inputFile));

        // Init SingleOutputStreamOperators for the three functionalities
        SpeedRadar speedControl = new SpeedRadar();
        AccidentReporter accidentsChecker = new AccidentReporter();
        AvgSpeedCheck avgSpeedChecker = new AvgSpeedCheck();

        // Run jobs
        DataStream<SpeedFine> OutputFines= speedControl.run(positionStream);
        DataStream<Accident> OutputAccidents = accidentsChecker.run(positionStream);
        DataStream<AvgSpeedFine> OutputAvgSpeedFines = avgSpeedChecker.run(positionStream);

        // Write final streams to output files, no parallelism since want only single files and no partitioned files
        OutputFines.writeAsText(outputFolder + "/" + SPEEDFINES, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
        OutputAccidents.writeAsText(outputFolder + "/" + ACCIDENTS, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
        OutputAvgSpeedFines.writeAsText(outputFolder + "/" + AVGSPEEDFINES, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        try {
            env.execute("VehicleTelematics");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

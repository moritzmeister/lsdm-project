package master2018.flink.source;

import master2018.flink.datatypes.PositionEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;

/**
 * PositionSource: A SourceFunction to create a stream of PositionEvents from an input file.
 * The source maps the strings to PositionEvent tuples and attaches Timestamps and strictly increasing watermarks.
 * The source function assumes that the input file is ordered by increasing timestamps.
 * ATTENTION: Both timestamps and watermarks are specified as milliseconds since the Java epoch of 1970-01-01T00:00:00Z.
 */

public class PositionSource implements SourceFunction<PositionEvent> {

    private final String inputFile;
    private transient BufferedReader reader;
    private transient InputStream fileStream;

    public PositionSource(String inputFile) {
        this.inputFile = inputFile;
    }

    @Override
    public void run(SourceContext<PositionEvent> sourceContext) throws Exception {
        System.out.println("Start DataSource");

        // Initialize file input stream and buffered reader
        fileStream = new FileInputStream(inputFile);
        reader = new BufferedReader(new InputStreamReader(fileStream, "UTF-8"));

        generateStream(sourceContext);

        // clean up
        this.reader.close();
        this.reader = null;
        this.fileStream.close();
        this.fileStream = null;
    }

    private void generateStream(SourceContext<PositionEvent> sourceContext) throws IOException {

        String line;
        PositionEvent data;
        Long count = 0L;
        Long currentEventTime = 0L;

        System.out.println("Generating stream: ");

        // Read one line at a time, transform it to PositionEvent and add it to the emit schedule
        while (reader.ready() && (line = reader.readLine()) != null) {

            String[] args = line.split(",");

            // Skip compromised rows in the input data and output them to the console
            if (args.length != 8){
                System.out.println("Error in input data: " + line);
            } else {
                data = new PositionEvent(args);

                sourceContext.collectWithTimestamp(data, (data.getTime()*1000));

                // Check if EventTime progressed and if it did, increase the current EventTime and emit Watermark
                if((data.getTime()*1000) > currentEventTime) {
                    currentEventTime = (data.getTime()*1000);
                    sourceContext.emitWatermark(new Watermark((data.getTime()*1000)));
                }

                // Security count to keep track of read tuples
                count += 1;
            }
        }
        System.out.println("Lines read: " + Long.toString(count));
    }

    @Override
    public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
            if (this.fileStream != null) {
                this.fileStream.close();
            }
        } catch(IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
            this.fileStream = null;
        }
    }
}

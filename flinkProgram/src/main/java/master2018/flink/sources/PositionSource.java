package master2018.flink.sources;

import master2018.flink.datatypes.PositionEvent;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.*;

public class PositionSource implements ParallelSourceFunction<PositionEvent> {

    private final String inputFile;
    private transient BufferedReader reader;
    private transient InputStream fileStream;

    public PositionSource(String inputFile) {
        this.inputFile = inputFile;
    }

    @Override
    public void run(SourceContext<PositionEvent> sourceContext) throws Exception {
        System.out.println("Start DataSource");

        fileStream = new FileInputStream(inputFile);
        reader = new BufferedReader(new InputStreamReader(fileStream, "UTF-8"));

        generateStream(sourceContext);

        // clean up
        this.reader.close();
        this.reader = null;
        this.fileStream.close();
        this.fileStream = null;
    }

    private void generateStream(SourceContext<PositionEvent> sourceContext) throws Exception {

        String line;
        PositionEvent data;
        Long count = 0L;

        System.out.println("Generating stream: ");

        /*
        *
        * ADD LOGIC TO EMIT TIMESTAMPS as EVENT TIME
        * 
         */
        while (reader.ready() && (line = reader.readLine()) != null) {
            data = new PositionEvent(line);
            sourceContext.collect(data);
            count += 1;
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

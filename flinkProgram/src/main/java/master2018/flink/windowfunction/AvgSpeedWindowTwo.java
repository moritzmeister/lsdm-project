package master2018.flink.windowfunction;

import master2018.flink.datatypes.AvgSpeedFine;
import master2018.flink.datatypes.PositionEvent;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import sun.util.resources.ga.LocaleNames_ga;

import java.util.ArrayList;
import java.util.Iterator;

public class AvgSpeedWindowTwo implements WindowFunction<PositionEvent, AvgSpeedFine,
        Tuple3<String, Integer, Integer>, TimeWindow> {

    private AvgSpeedFine output = new AvgSpeedFine();
    private int seg_start;
    private int seg_end;
    private double max_speed;
    private double conversion;

    public AvgSpeedWindowTwo(int start, int end, double max, double conv) {
        this.seg_start = start;
        this.seg_end = end;
        this.max_speed = max;
        this.conversion = conv;
    }

    @Override
    public void apply(Tuple3<String, Integer, Integer> key, TimeWindow  timeWindow,
                      Iterable<PositionEvent> iterable, Collector<AvgSpeedFine> collector) {

        Iterator<PositionEvent> events = iterable.iterator();

        PositionEvent currentElement;
        int currentSegment = -1;
        Long minStartTime = Long.MAX_VALUE;
        Long maxEndTime = 0L;
        PositionEvent firstElement = null;
        PositionEvent lastElement = null;

        double avgSpeed = 0;
        ArrayList<Integer> uniqueSegments = new ArrayList<>();

        try {

            while (events.hasNext()) {
                currentElement = events.next();
                currentSegment = currentElement.getSegment();

                if (!uniqueSegments.contains(currentSegment)) {
                    uniqueSegments.add(currentElement.getSegment());
                }

                if ((currentSegment == seg_start || currentSegment == seg_end)
                    && currentElement.getTime() < minStartTime) {
                    firstElement = currentElement;
                    minStartTime = currentElement.getTime();
                }
                else if ((currentSegment == seg_start || currentSegment == seg_end)
                        && currentElement.getTime() > maxEndTime) {
                    lastElement = currentElement;
                    maxEndTime = currentElement.getTime();
                }
            }

            if (uniqueSegments.size() == (seg_end - seg_start + 1)) {
                // calc speed
                avgSpeed = ((Math.abs(lastElement.getPosition() - firstElement.getPosition()) * 1.0)
                        / (lastElement.getTime() - firstElement.getTime())) * conversion;

                if (avgSpeed > max_speed) {
                    output.setTime1(firstElement.getTime());
                    output.setTime2(lastElement.getTime());
                    output.setVid(key.f0);
                    output.setXway(key.f1);
                    output.setDirection(key.f2);
                    output.setAvgSpeed(avgSpeed);
                    collector.collect(output);
                }
            }
        } catch (Exception e) {

        }
    }
}
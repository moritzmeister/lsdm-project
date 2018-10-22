package master2018.flink.datatypes;

public class PositionEvent {

    private Long time;
    private String vid;
    private int speed;
    private int xway;
    private int lane;
    private int direction;
    private int segment;
    private int position;

    private boolean hasWatermarkTime;
    private Long watermarkTime;

    public PositionEvent(String[] line){
        /* Constructs a CensusData Object from a comma separated string input. */
        String[] args = line;


        this.time = Long.parseLong(args[0]);
        this.vid = args[1];
        this.speed = Integer.parseInt(args[2]);
        this.xway = Integer.parseInt(args[3]);
        this.lane = Integer.parseInt(args[4]);
        this.direction = Integer.parseInt(args[5]);
        this.segment = Integer.parseInt(args[6]);
        this.position = Integer.parseInt(args[7]);
        this.hasWatermarkTime = false;
        this.watermarkTime = null;

    }

    public boolean hasWatermarkTime() {
        return this.hasWatermarkTime;
    }

    public Long getWatermarkTime() {
        return (this.watermarkTime * 1000);
    }

    public void setWatermark() {
        this.hasWatermarkTime = true;
        this.watermarkTime = this.time;
    }

    public Long getTime() {
        // Flink works in milliseconds
        return (this.time * 1000);
    }

    public String getVid() {
        return this.vid;
    }

    public int getSpeed() {
        return this.speed;
    }

    public int getXway() {
        return this.xway;
    }

    public int getLane() {
        return this.lane;
    }

    public int getDirection() {
        return this.direction;
    }

    public int getSegment() {
        return this.segment;
    }

    public int getPosition() {
        return this.position;
    }

    public String toString() {
        /* Returns the object attributes as a comma separated string */
        StringBuilder sb = new StringBuilder();
        sb.append(Long.toString(time)).append(",");
        sb.append(vid).append(",");
        sb.append(Integer.toString(speed)).append(",");
        sb.append(Integer.toString(xway)).append(",");
        sb.append(Integer.toString(lane)).append(",");
        sb.append(Integer.toString(direction)).append(",");
        sb.append(Integer.toString(segment)).append(",");
        sb.append(Integer.toString(position)).append(", WatermarkTime: ");
        sb.append(hasWatermarkTime).append(",");
        sb.append(watermarkTime);

        return sb.toString();
    }
}

package master2018.flink.datatypes;

public class SpeedFine {

    private Long time;
    private String vid;
    private int speed;
    private int xway;
    private int lane;
    private int direction;

    public SpeedFine(PositionEvent positionEvent) {
        this.time = positionEvent.getTime();
        this.vid = positionEvent.getVid();
        this.speed = positionEvent.getSpeed();
        this.xway = positionEvent.getXway();
        this.lane = positionEvent.getLane();
        this.direction = positionEvent.getDirection();
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public void setVid(String vid) {
        this.vid = vid;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }

    public void setXway(int xway) {
        this.xway = xway;
    }

    public void setLane(int lane) {
        this.lane = lane;
    }

    public void setDirection(int direction) {
        this.direction = direction;
    }

    public String toString() {
        /* Returns the object attributes as a comma separated string */
        StringBuilder sb = new StringBuilder();

        sb.append(Long.toString(time)).append(",");
        sb.append(vid).append(",");
        sb.append(Integer.toString(speed)).append(",");
        sb.append(Integer.toString(xway)).append(",");
        sb.append(Integer.toString(lane)).append(",");
        sb.append(Integer.toString(direction));

        return sb.toString();
    }
}

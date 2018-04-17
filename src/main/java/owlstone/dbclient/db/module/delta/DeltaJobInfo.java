package owlstone.dbclient.db.module.delta;

/**
 * Created by aj65 on 2017/3/14.
 */
public class DeltaJobInfo {
    private boolean running;
    private DeltaData deltaData;
    private Exception exception;

    public DeltaJobInfo() {
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public DeltaData getDeltaData() {
        return deltaData;
    }

    public void setDeltaData(DeltaData deltaData) {
        this.deltaData = deltaData;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }
}

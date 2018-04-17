package owlstone.dbclient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by aj65 on 2017/5/8.
 */
public class Debug {
    public static final Logger log_coon;
    static {
        log_coon = LogManager.getLogger("ConnectionInfo");
        //LogManager.shutdown();
    }
}

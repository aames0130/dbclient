package owlstone.dbclient.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by aj65
 *
 * This class has a Timer to invoke method removeDeadConn periodically.
 * execute one time every "managerConfig.checkDelay" seconds if "enableTimerChecker" is true.
 *
 * Default will check every 60 seconds and remove dead connections.
 * Checker will start after 5 min delay.
 *
 * You can use method setManagerConfig() and startCheck() to change the check period.
 * You can also just call stopCheck() to stop checking.
 *
 * If you want to pause checking temporarily, you can call method setEnableTimerChecker(false) and
 * call setEnableTimerChecker(true) to resume checker after that.
 */
public abstract class AutoRecycleConnectionManager{
    private final static Logger log = LogManager.getLogger(AutoRecycleConnectionManager.class);
    protected boolean enableTimerChecker = true;
    protected ManagerConfig managerConfig;
    protected Timer checkTimer;

    /**
     * it will try to start a new checker with new check interval and check delay
     * and if old checker existed, it will stop it first.
     */
    public void startCheck(){
        log.debug("checkDelay = {}",managerConfig.getCheckDelay());
        log.debug("checkPeriod = {}",managerConfig.getCheckPeriod());

        this.stopCheck();
        //
        // make checkTimer as daemon to let it auto terminate after main thread exit
        //
        checkTimer = new Timer("AutoRecycleCheckTimer",true);
        checkTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                if(enableTimerChecker)
                    removeDeadConn();
            }
        },managerConfig.getCheckDelay()*1000,managerConfig.getCheckPeriod()*1000);
    }

    /**
     * Cancel the Timer thread that periodically remove expire and dead connection.
     */
    public void stopCheck(){
        if( null!=checkTimer)
        {
            checkTimer.cancel();
            checkTimer = null;
        }
    }

    public abstract Connection getConnection(String dsName,boolean share);

    public Connection getConnection(String dsName){
        return getConnection(dsName,true);
    }

    public abstract void recycleConnection(String dsName,Connection connection);
    public abstract void removeDBConn(String dsName);
    public abstract void removeAllDBConn();
    public boolean isEnableTimerChecker() {
        return enableTimerChecker;
    }

    public void setEnableTimerChecker(boolean enableTimerChecker) {
        this.enableTimerChecker = enableTimerChecker;
    }


    public ManagerConfig getManagerConfig() {
        return managerConfig;
    }

    protected abstract void removeDeadConn();
}

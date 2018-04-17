package owlstone.dbclient.util;

// program time measure
public class PTM {
    public class Node{
        public long stime;
        public long etime;
    }

    private java.util.Map<String, Node> pdata = new java.util.HashMap<String, PTM.Node>( );

    public void start(String key){
        Node n = new Node();
        n.stime = System.currentTimeMillis();

        pdata.put(key,n);
    }

    /**
     *
     * @return milli sec
     */
    public long end(String key){
        long ret = -1;

        if( null!= pdata.get(key) )
        {
            Node n = pdata.get(key);
            n.etime = System.currentTimeMillis();
            ret = n.etime - n.stime;
        }

        return ret;
    }

    public String printPeriod(String key){
        Node node = pdata.get(key);
        if( null != node && ( node.stime >0 && node.etime >0 ) )
            return key+" takes "+String.valueOf( node.etime - node.stime ) +" milli sec.";
        else
            return key+" takes -1 milli sec.";
    }

    public String printPeriodSec(String key){
        Node node = pdata.get(key);
        if( null != node && ( node.stime >0 && node.etime >0 ) )
            return key+" takes "+String.valueOf( (node.etime - node.stime)/1000 ) +" secs.";
        else
            return key+" takes -1 milli sec.";
    }
}


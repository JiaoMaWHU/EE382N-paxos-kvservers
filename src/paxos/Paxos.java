package paxos;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is the main class you need to implement paxos instances.
 */
public class Paxos implements PaxosRMI, Runnable{

    ReentrantLock mutex;
    String[] peers; // hostname
    int[] ports; // host port
    int me; // index into peers[]

    Registry registry;
    PaxosRMI stub;

    AtomicBoolean dead;// for testing
    AtomicBoolean unreliable;// for testing

    // Your data here
    public class record {
        public int n_promise;
        public int n_accept;
        public Object v_accept;
        public State state;

        public record(){
            this.n_promise = -1;
            this.n_accept = -1;
            this.v_accept = null;
            this.state = State.Pending;
        }
    }

    int peers_length;
    int majority;

    HashMap<Integer, record> records;

    int seq;
    Object v_proposal;

    int done_seq;
    int[] peers_done_seq;

    /**
     * Call the constructor to create a Paxos peer.
     * The hostnames of all the Paxos peers (including this one)
     * are in peers[]. The ports are in ports[].
     */
    public Paxos(int me, String[] peers, int[] ports){

        this.me = me;
        this.peers = peers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.dead = new AtomicBoolean(false);
        this.unreliable = new AtomicBoolean(false);

        // Your initialization code here
        peers_length = peers.length;
        majority = (peers_length /2) + 1;
        records = new HashMap<Integer, record>();
        seq = -1;
        v_proposal = null;
        done_seq = -1;
        peers_done_seq = new int[peers_length];
        for (int i = 0; i<peers_length; i++)
            peers_done_seq[i] = -1;

        // register peers, do not modify this part
        try{
            System.setProperty("java.rmi.server.hostname", this.peers[this.me]);
            registry = LocateRegistry.createRegistry(this.ports[this.me]);
            stub = (PaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("Paxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }


    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     *
     * You should assume that Call() will time out and return
     * null after a while if it doesn't get a reply from the server.
     *
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id){
        Response callReply = null;

        PaxosRMI stub;
        try{
            Registry registry=LocateRegistry.getRegistry(this.ports[id]);
            stub=(PaxosRMI) registry.lookup("Paxos");
            if(rmi.equals("Prepare"))
                callReply = stub.Prepare(req);
            else if(rmi.equals("Accept"))
                callReply = stub.Accept(req);
            else if(rmi.equals("Decide"))
                callReply = stub.Decide(req);
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            return null;
        }
        return callReply;
    }


    /**
     * The application wants Paxos to start agreement on instance seq,
     * with proposed value v. Start() should start a new thread to run
     * Paxos on instance seq. Multiple instances can be run concurrently.
     *
     * Hint: You may start a thread using the runnable interface of
     * Paxos object. One Paxos object may have multiple instances, each
     * instance corresponds to one proposed value/command. Java does not
     * support passing arguments to a thread, so you may reset seq and v
     * in Paxos object before starting a new thread. There is one issue
     * that variable may change before the new thread actually reads it.
     * Test won't fail in this case.
     *
     * Start() just starts a new thread to initialize the agreement.
     * The application will call Status() to find out if/when agreement
     * is reached.
     */
    public void Start(int seq, Object value){
        mutex.lock();
        try {
            if (seq < Min()) {
                return;
            }
            this.seq = seq;
            this.v_proposal = value;
        } finally {
            mutex.unlock();
        }
        Thread t = new Thread(this);
        t.start();
    }

    @Override
    public void run(){
        int n_proposal_;
        int highest_n_sofar = -1;
        int seq_;
        int cp_done_seq;
        Object v_proposal_;

        mutex.lock();
        try {
            seq_ = seq;
            v_proposal_ = v_proposal;
        } finally {
            mutex.unlock();
        }

        while (true) {
            mutex.lock();
            try {
                if (records.get(seq_) != null && records.get(seq_).state == State.Decided )
                    break;
            }finally {
                mutex.unlock();
            }

            n_proposal_ = highest_n_sofar+1;
            highest_n_sofar++;

            // propose
            Response[] responses = new Response[peers_length];
            mutex.lock();
            try {
                cp_done_seq = done_seq;
            }finally {
                mutex.unlock();
            }
            Request prepare_req = new Request(seq_, n_proposal_, null, cp_done_seq, me);
            for (int i = 0; i < peers_length; i++) {
                if (i != me)
                    responses[i] = Call("Prepare", prepare_req, i);
                else
                    responses[i] = Prepare(prepare_req);
            }

            int max_n_a = -1;
            int good_responses = 0;
            Object v_a = null;
            Object v_prime = null;
            for (Response res : responses) {
                if (res == null) {
                    continue;
                }
                highest_n_sofar = Math.max(highest_n_sofar, res.n_a);
                if (res.n == n_proposal_) {
                    good_responses++;
                    if (res.n_a > max_n_a) {
                        max_n_a = res.n_a;
                        v_a = res.v_a;
                    }
                }
            }

            if (good_responses < majority) {
                continue;
            }

            v_prime = v_a == null ? v_proposal_ : v_a;

            // accept
            mutex.lock();
            try {
                cp_done_seq = done_seq;
            }finally {
                mutex.unlock();
            }
            Request accept_req = new Request(seq_, n_proposal_, v_prime, cp_done_seq, me);
            for (int i = 0; i < peers_length; i++) {
                if (i != me)
                    responses[i] = Call("Accept", accept_req, i);
                else
                    responses[i] = Accept(accept_req);
            }
            good_responses = 0;
            for (Response res : responses) {
                if (res == null) {
                    continue;
                }
                if (res.n == n_proposal_) {
                    good_responses++;
                }
            }

            if (good_responses < majority) {
                continue;
            }

            // decide
            mutex.lock();
            try {
                cp_done_seq = done_seq;
            }finally {
                mutex.unlock();
            }
            Request decide_req = new Request(seq_, n_proposal_, v_prime, cp_done_seq, me);
            for (int i = 0; i < peers_length; i++) {
                if (i != me)
                    responses[i] = Call("Decide", decide_req, i);
                else
                    responses[i] = Decide(decide_req);
            }
        }
    }

    // RMI handler
    public Response Prepare(Request req){
        mutex.lock();
        try {
            if (records.get(req.seq) == null)
                records.put(req.seq, new record());
            peers_done_seq[req.me] = req.done_seq;
            if (req.n > records.get(req.seq).n_promise) {
                records.get(req.seq).n_promise = req.n;
                return new Response(req.n, records.get(req.seq).n_accept, records.get(req.seq).v_accept);
            }else{
                return new Response(-1, -1, null);
            }
        }finally {
            mutex.unlock();
        }
    }

    public Response Accept(Request req){
        mutex.lock();
        try {
            peers_done_seq[req.me] = req.done_seq;
            if (req.n >= records.get(req.seq).n_promise) {
                records.get(req.seq).n_promise = req.n;
                records.get(req.seq).n_accept = req.n;
                records.get(req.seq).v_accept = req.v;
                return new Response(req.n, records.get(req.seq).n_accept, records.get(req.seq).v_accept);
            }else{
                return new Response(-1, -1, null);
            }
        }finally {
            mutex.unlock();
        }

    }

    public Response Decide(Request req){
        mutex.lock();
        try {
            peers_done_seq[req.me] = req.done_seq;
            records.get(req.seq).state = State.Decided;
            records.get(req.seq).v_accept = req.v;
        }finally {
            mutex.unlock();
        }
        return new Response(req.n, records.get(req.seq).n_accept, records.get(req.seq).v_accept);
    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     *
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        mutex.lock();
        try {
            done_seq = Math.max(done_seq, seq);
        }finally {
            mutex.unlock();
        }
    }


    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max(){
        int max_seq = -1;
        mutex.lock();
        try {
            if (records.isEmpty()) return -1;
            max_seq = Collections.max(records.keySet());
        }finally {
            mutex.unlock();
        }
        return max_seq;
    }

    /**
     * Min() should return one more than the minimum among z_i,
     * where z_i is the highest number ever passed
     * to Done() on peer i. A peers z_i is -1 if it has
     * never called Done().

     * Paxos is required to have forgotten all information
     * about any instances it knows that are < Min().
     * The point is to free up memory in long-running
     * Paxos-based servers.

     * Paxos peers need to exchange their highest Done()
     * arguments in order to implement Min(). These
     * exchanges can be piggybacked on ordinary Paxos
     * agreement protocol messages, so it is OK if one
     * peers Min does not reflect another Peers Done()
     * until after the next instance is agreed to.

     * The fact that Min() is defined as a minimum over
     * all Paxos peers means that Min() cannot increase until
     * all peers have been heard from. So if a peer is dead
     * or unreachable, other peers Min()s will not increase
     * even if all reachable peers call Done. The reason for
     * this is that when the unreachable peer comes back to
     * life, it will need to catch up on instances that it
     * missed -- the other peers therefore cannot forget these
     * instances.
     */
    public int Min(){
        int min = Integer.MAX_VALUE;
        mutex.lock();
        try {
            for (int i: peers_done_seq)
                min = Math.min(min, i);
            for (int key: records.keySet()) {
                if (key <= min) {
                    records.get(key).state = State.Forgotten;
                }
            }
        }finally {
            mutex.unlock();
        }
        return min+1;
    }



    /**
     * the application wants to know whether this
     * peer thinks an instance has been decided,
     * and if so what the agreed value is. Status()
     * should just inspect the local peer state;
     * it should not contact other Paxos peers.
     */
    public retStatus Status(int seq){
        // Your code here
        this.Min();
        retStatus ret = null;
        mutex.lock();
        try {
            if (records.get(seq) == null) {
                return new retStatus(State.Pending, null);
            }
            ret = new retStatus(records.get(seq).state, records.get(seq).v_accept);
        }finally {
            mutex.unlock();
        }
        return ret;
    }

    /**
     * helper class for Status() return
     */
    public class retStatus{
        public State state;
        public Object v;

        public retStatus(State state, Object v){
            this.state = state;
            this.v = v;
        }
    }

    /**
     * Tell the peer to shut itself down.
     * For testing.
     * Please don't change these four functions.
     */
    public void Kill(){
        this.dead.getAndSet(true);
        if(this.registry != null){
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
            } catch(Exception e){
                System.out.println("None reference");
            }
        }
    }

    public boolean isDead(){
        return this.dead.get();
    }

    public void setUnreliable(){
        this.unreliable.getAndSet(true);
    }

    public boolean isunreliable(){
        return this.unreliable.get();
    }


}

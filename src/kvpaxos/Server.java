package kvpaxos;
import paxos.Paxos;
import paxos.State;
// You are allowed to call Paxos.Status to check if agreement was made.

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class Server implements KVPaxosRMI {

    ReentrantLock mutex;
    Registry registry;
    Paxos px;
    int me;

    String[] servers;
    int[] ports;
    KVPaxosRMI stub;

    // Your definitions here
    Map<String, Integer> keyValMap;
    int curSeq;
    Set<Integer> pastRequests;


    public Server(String[] servers, int[] ports, int me){
        this.me = me;
        this.servers = servers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.px = new Paxos(me, servers, ports);
        // Your initialization code here
        this.keyValMap = new HashMap<>();
        this.curSeq = 0;
        this.pastRequests = new HashSet<>();

        try{
            System.setProperty("java.rmi.server.hostname", this.servers[this.me]);
            registry = LocateRegistry.getRegistry(this.ports[this.me]);
            stub = (KVPaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("KVPaxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    public Op wait(int seq) {
        int to = 10;
        while (true) {
            Paxos.retStatus ret = this.px.Status(seq);
            if (ret.state == State.Decided) {
                return (Op) ret.v;
            }
            try {
                Thread.sleep(to);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (to < 1000) {
                to = to * 2;
            }
        }
    }

    private void updateKeyValMap(int maxSeq) {
        for (int i = curSeq; i <= maxSeq; i++) {
            if (px.Status(i).state == State.Decided) {
                Op oPair = (Op) px.Status(i).v;
                if (oPair.op.equals("Put")) {
                    keyValMap.put(oPair.key, oPair.value);
                }
                pastRequests.add(oPair.ClientSeq);
            }
        }
    }

    // RMI handlers
    public Response Get(Request req){
        // Your code here
        try {
            mutex.lock();
            int maxSeq = px.Max();
            updateKeyValMap(maxSeq);

            int ClientSeq = req.oPair.ClientSeq;
            String key = req.oPair.key;

            if (pastRequests.contains(ClientSeq)) {
                Integer val = keyValMap.getOrDefault(key, null);
                return new Response(key, val, true);
            }

            pastRequests.add(ClientSeq);
            px.Start(ClientSeq, req.oPair);
            wait(ClientSeq);


            px.Done(maxSeq);
            curSeq = maxSeq + 1;

            Integer val = keyValMap.getOrDefault(key, null);
            return new Response(key, val, true);
        } finally {
            mutex.unlock();
        }
    }

    public Response Put(Request req){
        // Your code here
        try {
            mutex.lock();
            int maxSeq = px.Max();
            updateKeyValMap(maxSeq);

            int ClientSeq = req.oPair.ClientSeq;
            if (pastRequests.contains(ClientSeq)) {
                return new Response(req.oPair.key, req.oPair.value, true);
            }

            pastRequests.add(ClientSeq);
            px.Start(ClientSeq, req.oPair);
            wait(ClientSeq);

            px.Done(maxSeq);

            curSeq = maxSeq + 1;

            return new Response(req.oPair.key, req.oPair.value, false);

        } finally {
            mutex.unlock();
        }
    }


}

package paxos;
import java.io.Serializable;

/**
 * Please fill in the data structure you use to represent the response message for each RMI call.
 * Hint: You may need a boolean variable to indicate ack of acceptors and also you may need proposal number and value.
 * Hint: Make it more generic such that you can use it for each RMI call.
 */
public class Response implements Serializable {
    static final long serialVersionUID=2L;
    // your data here
    public int n;
    public int n_a;
    public Object v_a;


    // Your constructor and methods here
    public Response(int n_, int n_a_, Object v_a_){
        n = n_;
        n_a = n_a_;
        v_a = v_a_;
    }
}

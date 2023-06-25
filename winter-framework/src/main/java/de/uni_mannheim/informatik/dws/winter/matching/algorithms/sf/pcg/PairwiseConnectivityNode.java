package de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg;

/**
 * @author Robin Schumacher (info@robin-schumacher.com
 */
public class PairwiseConnectivityNode {

    SFNode a;
    SFNode b;

    public PairwiseConnectivityNode(SFNode a, SFNode b) {
        this.a = a;
        this.b = b;
    }

    public SFNode getA() {
        return a;
    }

    public void setA(SFNode a) {
        this.a = a;
    }

    public SFNode getB() {
        return b;
    }

    public void setB(SFNode b) {
        this.b = b;
    }
}

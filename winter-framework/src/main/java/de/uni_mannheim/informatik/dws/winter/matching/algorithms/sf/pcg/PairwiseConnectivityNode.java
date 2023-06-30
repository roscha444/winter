package de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg;

/**
 * @author Robin Schumacher (info@robin-schumacher.com
 */
public class PairwiseConnectivityNode<T> {

    SFNode<T> a;
    SFNode<T> b;

    public PairwiseConnectivityNode(SFNode<T> a, SFNode<T> b) {
        this.a = a;
        this.b = b;
    }

    public SFNode<T> getA() {
        return a;
    }

    public void setA(SFNode<T> a) {
        this.a = a;
    }

    public SFNode<T> getB() {
        return b;
    }

    public void setB(SFNode<T> b) {
        this.b = b;
    }
}

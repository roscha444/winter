package de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg;

import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.PairwiseConnectivityNode;

/**
 * @author Robin Schumacher (info@robin-schumacher.com
 */
public class IPGNode {

    PairwiseConnectivityNode pairwiseConnectivityNode;
    double initSim;
    double currSim;
    double nextSim;

    public IPGNode(PairwiseConnectivityNode pairwiseConnectivityNode, double initSim, double currSim, double nextSim) {
        this.pairwiseConnectivityNode = pairwiseConnectivityNode;
        this.initSim = initSim;
        this.currSim = currSim;
        this.nextSim = nextSim;
    }

    public PairwiseConnectivityNode getPairwiseConnectivityNode() {
        return pairwiseConnectivityNode;
    }

    public void setPairwiseConnectivityNode(PairwiseConnectivityNode pairwiseConnectivityNode) {
        this.pairwiseConnectivityNode = pairwiseConnectivityNode;
    }

    public double getInitSim() {
        return initSim;
    }

    public void setInitSim(double initSim) {
        this.initSim = initSim;
    }

    public double getCurrSim() {
        return currSim;
    }

    public void setCurrSim(double currSim) {
        this.currSim = currSim;
    }

    public double getNextSim() {
        return nextSim;
    }

    public void setNextSim(double nextSim) {
        this.nextSim = nextSim;
    }
}

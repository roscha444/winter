package de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf;

import org.jgrapht.graph.DefaultEdge;

public class CoeffEdge extends DefaultEdge {

    double coeff;

    public CoeffEdge(double coeff) {
        this.coeff = coeff;
    }

    public double getCoeff() {
        return coeff;
    }

    public void setCoeff(double coeff) {
        this.coeff = coeff;
    }
}

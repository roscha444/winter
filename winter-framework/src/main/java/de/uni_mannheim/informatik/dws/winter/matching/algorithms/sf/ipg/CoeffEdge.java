package de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg;

import org.jgrapht.graph.DefaultEdge;

/**
 * @author Robin Schumacher (info@robin-schumacher.com
 */
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

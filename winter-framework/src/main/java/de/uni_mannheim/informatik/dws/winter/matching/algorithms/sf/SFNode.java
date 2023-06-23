package de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf;

/**
 * This class represents a node in the graph representation for a relational schema according to the SF-algorithm.
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public abstract class SFNode {

    protected String value;

    public SFNode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}

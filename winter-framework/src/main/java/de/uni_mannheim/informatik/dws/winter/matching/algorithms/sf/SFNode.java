package de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf;

/**
 * This class represents a node in the graph representation for a relational schema according to the SF-algorithm.
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class SFNode {

    protected String value;

    private final SFNodeType type;

    public SFNode(String value, SFNodeType type) {
        this.value = value;
        this.type = type;
    }

    public SFNodeType getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}

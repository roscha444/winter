package de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg;

/**
 * This class represents a node in the graph representation for a relational schema according to the SF-algorithm.
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class SFNode<TypeA> {

    private String getIdentifier;
    private final SFNodeType type;
    private TypeA matchable;

    public SFNode(String getIdentifier, SFNodeType type) {
        this.getIdentifier = getIdentifier;
        this.type = type;
        this.matchable = null;
    }

    public SFNode(String getIdentifier, SFNodeType type, TypeA matchable) {
        this.getIdentifier = getIdentifier;
        this.type = type;
        this.matchable = matchable;
    }

    public TypeA getMatchable() {
        return matchable;
    }

    public void setMatchable(TypeA matchable) {
        this.matchable = matchable;
    }

    public SFNodeType getType() {
        return type;
    }

    public String getGetIdentifier() {
        return getIdentifier;
    }

    public void setGetIdentifier(String getIdentifier) {
        this.getIdentifier = getIdentifier;
    }
}

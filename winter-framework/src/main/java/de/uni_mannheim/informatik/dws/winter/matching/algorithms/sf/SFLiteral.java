package de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf;

/**
 * This class represents a literal or string value in the sf graph representation.
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class SFLiteral extends SFNode {

    public SFLiteral(String literal) {
        super(literal);
    }

    @Override
    public String toString() {
        return "SFLiteral{" +
            "value='" + value + '\'' +
            '}';
    }
}

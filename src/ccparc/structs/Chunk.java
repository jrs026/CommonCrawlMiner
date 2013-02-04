// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.structs;

public abstract class Chunk {

    public final String content;

    public Chunk (String content) {
        this.content = normalize (content);
    }

    public abstract String normalize (String content);

    public abstract boolean canAlignWith (Chunk o);

    public int hashCode () {
        return content.hashCode();
    }

    public boolean equals (Object o) {
        return (
            o.getClass() == getClass()
            && ((Chunk) o).content.equals (content)
        );
    }

    public String toString () {
        return content;
    }

}

//---------------------------------------------------------------------------------------------------------------------------------

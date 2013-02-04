// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.structs;

//---------------------------------------------------------------------------------------------------------------------------------

public class CandidatePair {

    public final Candidate enCandidate;
    public final Candidate frCandidate;

    public CandidatePair (Candidate enCandidate, Candidate frCandidate) {
        this.enCandidate = enCandidate;
        this.frCandidate = frCandidate;
    }

    public String toString () {
        return enCandidate + "\n" + frCandidate + "\n";
    }

}

//---------------------------------------------------------------------------------------------------------------------------------

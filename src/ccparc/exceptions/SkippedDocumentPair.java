// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.exceptions;

//---------------------------------------------------------------------------------------------------------------------------------

public class SkippedDocumentPair
    extends Exception
{

    public enum Reason {
        MISALIGNED,
        CONTAINS_NO_TEXT
    };

    public static final long serialVersionUID = 1L;

    public final String enUrl;
    public final String frUrl;
    public final Reason reason;
    public final String details;

    public SkippedDocumentPair (String enUrl, String frUrl, Reason reason) {
        this (enUrl, frUrl, reason, null);
    }

    public SkippedDocumentPair (String enUrl, String frUrl, Reason reason, String details) {
        super ("Skipped '" + enUrl + "' <> '" + frUrl + "' (" + reason + (details == null ? "" : (" - " + details)) + ")");
        this.enUrl = enUrl;
        this.frUrl = frUrl;
        this.reason = reason;
        this.details = details;
    }

}

//---------------------------------------------------------------------------------------------------------------------------------

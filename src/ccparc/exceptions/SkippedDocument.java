// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.exceptions;

//---------------------------------------------------------------------------------------------------------------------------------

public class SkippedDocument
    extends Exception
{

    public enum Reason {
        NOT_HTML,
        TOO_LARGE,
        FAILED_LANGUAGE_TEST
    }

    public static final long serialVersionUID = 1L;

    public final String url;
    public final Reason reason;
    public final String details;

    public SkippedDocument (String url, Reason reason) {
        this (url, reason, null);
    }

    public SkippedDocument (String url, Reason reason, String details) {
        super ("Skipped '" + url + "' (" + reason + (details == null ? "" : (" - " + details)) + ")");
        this.url = url;
        this.reason = reason;
        this.details = details;
    }

}

//---------------------------------------------------------------------------------------------------------------------------------

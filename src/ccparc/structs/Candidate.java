// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.structs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

//---------------------------------------------------------------------------------------------------------------------------------

public class Candidate
    implements Writable
{

    public final LanguageIndependentUrl lui;
    public final Text html;


    public Candidate () {
        this (new LanguageIndependentUrl(), new Text());
    }

    public Candidate (LanguageIndependentUrl lui, String html) {
        this (lui, new Text (html));
    }

    public Candidate (LanguageIndependentUrl lui, Text html) {
        this.lui = lui;
        this.html = html;
    }


    public Candidate copy () {
        return new Candidate (
            lui.copy(),
            new Text (html)
        );
    }


    public int hashCode () {
        throw new UnsupportedOperationException ("This class is mutable, don't hash it");
    }

    public void readFields (DataInput in) throws IOException {
        lui.readFields (in);
        html.readFields (in);
    }

    public void write (DataOutput out) throws IOException {
        lui.write (out);
        html.write (out);
    }


    public String toString () {
        return lui.toString();
    }

}

//---------------------------------------------------------------------------------------------------------------------------------

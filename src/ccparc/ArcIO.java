// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.http.HttpException;

import org.apache.http.entity.ContentType;

import org.commoncrawl.compressors.gzip.GzipCompressorInputStream;

import org.commoncrawl.hadoop.mapred.ArcRecord;

//---------------------------------------------------------------------------------------------------------------------------------

/** Static utils for reading ARC files. */
public class ArcIO {

    //-----------------------------------------------------------------------------------------------------------------------------
    // Main method

    public static Iterable<ArcRecord> parseRecords (final File file) {
        final byte[] _checkBuffer = new byte[64];
        return new Iterable<ArcRecord> () {
            public Iterator<ArcRecord> iterator () {
                return new Iterator<ArcRecord> () {

                    GzipCompressorInputStream gzip;
                    ArcRecord _next;

                    {
                        try {
                            gzip = new GzipCompressorInputStream (new FileInputStream (file));
                            // First record should be an ARC file header record.  Skip it.
                            this.skipRecord();
                            advance();
                        } catch (IOException ex) {
                            throw new Error (ex);
                        }
                    }

                    private void advance () {
                        _next = null;
                        try {
                            Text key = new Text ();
                            ArcRecord rec = new ArcRecord ();
                            if (next (key, rec))
                                _next = rec;
                        } catch (Exception ex) {
                            throw new Error (ex);
                        }
                    }

                    // Copy-pasted from ArcRecordReader
                    private void skipRecord()
                        throws IOException
                    {
                        long n = 0;
                        do {
                            n = this.gzip.skip(999999999);
                        } while (n > 0);
                        this.gzip.nextMember();
                    }

                    // This copy-pasted from ArcRecordReader
                    private synchronized boolean next (Text key, ArcRecord value)
                        throws IOException
                    {
                        boolean isValid = true;
    
                        // try reading an ARC record from the stream
                        try {
                            isValid = value.readFrom (this.gzip);
                        } catch (EOFException ex) {
                            return false;
                        }

                        // if the record is not valid, skip it
                        if (isValid == false) {
                            this.skipRecord();
                            return true;
                        }

                        if (value.getURL() != null)
                            key.set(value.getURL());

                        // check to make sure we've reached the end of the GZIP member
                        int n = this.gzip.read(_checkBuffer, 0, 64);

                        if (n != -1) {
                            this.skipRecord();
                        } else {
                            this.gzip.nextMember();
                        }
   
                        return true;
                    }

                    public boolean hasNext () {
                        return _next != null;
                    }

                    public ArcRecord next () {
                        ArcRecord ret = _next;
                        advance();
                        return ret;
                    }

                    public void remove () {
                        throw new Error ("no");
                    }
                };
            }
        };
    }

    //-----------------------------------------------------------------------------------------------------------------------------
    // utils

    public static InputStream recordInputStream (ArcRecord rec)
        throws IOException
    {
        try {
            return rec.getHttpResponse().getEntity().getContent();
        } catch (HttpException ex) {
            throw new IOException (ex);
        }
    }

    public static String getCharsetNameFromHttpHeaders (ArcRecord rec) {
        try {
            return ContentType.getOrDefault(rec.getHttpResponse().getEntity()).getCharset().name();
        } catch (Throwable ex) {
            return null;
        }
    }

    //-----------------------------------------------------------------------------------------------------------------------------

}

//---------------------------------------------------------------------------------------------------------------------------------

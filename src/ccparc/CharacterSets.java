// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.SequenceInputStream;

import java.nio.ByteBuffer;

import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

//---------------------------------------------------------------------------------------------------------------------------------

public class CharacterSets {

    private static final Pattern RE_META_CONTENT_TYPE = Pattern.compile (
        "<meta \\s (?:[^>]*?\\s)? http-equiv \\s*=\\s* [\'\"]\\s* content-type \\s*[\'\"] [^>]* >",
        Pattern.CASE_INSENSITIVE | Pattern.COMMENTS
    );

    private static final Pattern RE_CONTENT_TYPE_CHARSET = Pattern.compile (
        "; \\s* (?: charset \\s*=\\s* )+ ([\\w\\-]+)",
        Pattern.CASE_INSENSITIVE | Pattern.COMMENTS
    );


    public static final Map<String,String> CHARSET_ALIASES; static {
        Map<String,String> aliases = new HashMap<String,String> ();

        // This is what browsers do. Windows-1252 is a superset of ISO-8859-1 anyway, it just has fewer undefined codepoints.
        aliases.put ("ISO-8859-1", "Windows-1252");

        // Common typos, non-standard names
        aliases.put ("8859-1", "Windows-1251");
        aliases.put ("CP-1251", "Windows-1251");
        aliases.put ("CP-1252", "Windows-1252");
        aliases.put ("ISO-8559", "Windows-1252");
        aliases.put ("ISO-8559-1", "Windows-1252");
        aliases.put ("ISO85591", "Windows-1252");
        aliases.put ("LATIN-1", "Windows-1252");
        aliases.put ("UFT-8", "UTF-8");
        aliases.put ("UTF8", "UTF-8");
        aliases.put ("WIN-1251", "Windows-1251");
        aliases.put ("WIN-1252", "Windows-1252");
        aliases.put ("WINDOWS1251", "Windows-1251");
        aliases.put ("WINDOWS1252", "Windows-1252");

        CHARSET_ALIASES = Collections.unmodifiableMap (aliases);
    }
        

    //-----------------------------------------------------------------------------------------------------------------------------
    // public interface

    public static Reader decodeHtml (InputStream bytes, String charsetNameFromHttpHeaders)
        throws IOException
    {

        // If a charset was declared in the HTTP headers, assume it's correct and take it, that's the cheapest of all outcomes
        Charset charsetFromHttpHeaders = charsetByNameIfExists (charsetNameFromHttpHeaders);
        if (charsetFromHttpHeaders != null)
            return new InputStreamReader (bytes, charsetFromHttpHeaders);

        // Else we're going to have to take a peek at the document contents. We read off the top of the document into memory. The
        // size of the buffer should be big enough to include most charset declarations, and to get a feel of what character set
        // the document contains, but without being uselessly large.
        // 
        // 2012-09-18 - I looked at a 38GB sample of the Common Crawl, and 99.62% of documents having a character set declared in a
        // <meta> tag had it contained within the first 5000 chars (NB not bytes, so this could be a little off)
        // 
        byte[] headerBytes = new byte [5000];
        int headerLength = fillBuffer (bytes, headerBytes);
        String headerAsAscii = new String (headerBytes, "US-ASCII");
        bytes = concat (headerBytes, bytes);

        // If we can find a charset declaration in this header, assume it's correct and use it. It's cheaper than to go into
        // auto-detect.
        Charset charsetFromMetaTag = charsetByNameIfExists (getCharsetNameFromMetaTag (headerAsAscii));
        if (charsetFromMetaTag != null)
            return new InputStreamReader (bytes, charsetFromMetaTag);

        // Else, if no charset is defined, try to guess. If the document can decode as UTF-8, then it probably is UTF-8, as there
        // are very few byte sequences in other charsets that are both valid UTF-8 and likely to appear in a document (as far as I
        // know). Otherwise, fall back to Windows-1252 (Latin-1).
        // 
        // TODO proper probabilistic charset detection! Shouldn't be that hard
        // 
        if (charsetCanDecode (headerBytes, headerLength, "UTF-8"))
            return new InputStreamReader (bytes, "UTF-8");
        return new InputStreamReader (bytes, "Windows-1252");
    }


    //-----------------------------------------------------------------------------------------------------------------------------
    // helpers

    private static Charset charsetByNameIfExists (String name) {
        if (name != null) {
            try {
                return Charset.forName (canonicalCharsetName(name));
            } catch (IllegalArgumentException ex) {
                synchronized (System.out) {
                    System.out.println ("Unknown charset '" + name + "'");
                }
            }
        }
        return null;
    }

    private static String canonicalCharsetName (String name) {
        for (;;) {
            String alias = CHARSET_ALIASES.get (name.toUpperCase());
            if (alias == null)
                break;
            name = alias;
        }
        return name;
    }

    private static String getCharsetNameFromMetaTag (String headerAsAscii) {
        Matcher matcher = RE_META_CONTENT_TYPE.matcher (headerAsAscii);
        if (matcher.find()) {
            matcher = RE_CONTENT_TYPE_CHARSET.matcher (matcher.group());
            if (matcher.find())
                return matcher.group(1);
        }
        return null;
    }

    private static boolean charsetCanDecode (byte[] bytes, int length, String charsetName) {
        try {
            Charset charset = Charset.forName (charsetName);

            ByteBuffer buf = ByteBuffer.allocate (length);
            buf.put (bytes, 0, length);
            buf.rewind();

            // This will throw an IOException if the bytes can't decode as the given charset
            charset.newDecoder()
                .onMalformedInput (CodingErrorAction.REPORT)
                .onUnmappableCharacter (CodingErrorAction.REPORT)
                .decode (buf);

            return true;
        } catch (IOException ex) {
            return false;
        }
    }


    //-----------------------------------------------------------------------------------------------------------------------------
    // Lower-level I/O routines

    private static int fillBuffer (InputStream input, byte[] buf)
        throws IOException
    {
        int offset = 0;
        while (offset < buf.length) {
            int n = input.read (buf, offset, buf.length-offset);
            if (n < 0)
                break;
            offset += n;
        }
        return offset;
    }

    private static InputStream concat (byte[] header, InputStream rest) {
        return new SequenceInputStream (
            new ByteArrayInputStream (header),
            rest
        );
    }

}

//---------------------------------------------------------------------------------------------------------------------------------

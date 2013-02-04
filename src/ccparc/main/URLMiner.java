package ccparc.main;

// Java classes
import java.lang.IllegalArgumentException;
import java.lang.OutOfMemoryError;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// log4j classes
import org.apache.log4j.Logger;

// Hadoop classes
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
// Common Crawl classes
import org.commoncrawl.hadoop.mapred.ArcInputFormat;
import org.commoncrawl.hadoop.mapred.ArcRecord;
import ccparc.main.MiningRecord;

// Herve's code
import ccparc.structs.Language;
import ccparc.structs.LanguageIndependentUrl;

import ccparc.CCParc;
import ccparc.exceptions.SkippedDocument;

/**
 * An example showing how to analyze the Common Crawl ARC web content files.
 * 
 * @author Chris Stephens <chris@commoncrawl.org>
 */
public class URLMiner
    extends    Configured
    implements Tool {

  private static final Logger LOG = Logger.getLogger(URLMiner.class);

  /**
   * Maps incoming web documents to a list of Microformat 'itemtype' tags.
   * Filters out any non-HTML pages.
   *
   * @author Chris Stephens <chris@commoncrawl.org>
   *
   * Inspired by:
   *
   * @author Manu Sporny 
   * @author Steve Salevan
   */
  public static class URLMinerMapper
      extends    MapReduceBase
      implements Mapper<Text, ArcRecord, Text, Text> {
 
    // create a counter group for Mapper-specific statistics
    private final String _counterGroup = "Custom Mapper Counters";
    
    // Initialize the URL matcher
    public static final Map<String,Language> REVERSE_LANGUAGE_IDS;
    public static final Pattern RE_LANGUAGE_IDS;
    static {
      // TODO: Read a TSV resource file
      Map<String,List<String>> languageIds = new HashMap<String,List<String>> ();
      languageIds.put("English", Collections.unmodifiableList(
    		  Arrays.asList("English","en","eng")));
      languageIds.put("Spanish", Collections.unmodifiableList(
    		  Arrays.asList ("es", "esp", "spa", "spanish", "espanol")));
      languageIds.put("French", Collections.unmodifiableList (
    		  Arrays.asList ("fr", "fre", "fra", "french")));
      languageIds.put("German", Collections.unmodifiableList (
    		  Arrays.asList ("de", "deu", "ger", "german", "deutsch")));
      languageIds.put("Korean", Collections.unmodifiableList(
    		  Arrays.asList("Korean","ko","kor")));
      
      languageIds.put("Arabic", Collections.unmodifiableList(
    		  Arrays.asList("Arabic","ar","ara")));
      languageIds.put("Bengali", Collections.unmodifiableList(
    		  Arrays.asList("Bengali","bn","ben")));
      languageIds.put("Bulgarian", Collections.unmodifiableList(
    		  Arrays.asList("Bulgarian","bg","bul")));
//      languageIds.put("Indonesian", Collections.unmodifiableList(
//    		  Arrays.asList("Indonesian","Bahasa Indonesia","id","ind")));
      languageIds.put("Japanese", Collections.unmodifiableList(
    		  Arrays.asList("Japanese","ja","jpn")));
      languageIds.put("Kannada", Collections.unmodifiableList(
    		  Arrays.asList("Kannada","kn","kan")));
      languageIds.put("Russian", Collections.unmodifiableList(
    		  Arrays.asList("Russian","ru","rus","rus")));
      languageIds.put("Tamil", Collections.unmodifiableList(
    		  Arrays.asList("Tamil","ta","tam")));
      languageIds.put("Telugu", Collections.unmodifiableList(
    		  Arrays.asList("Telugu","te","tel")));
      languageIds.put("Urdu", Collections.unmodifiableList(
    		  Arrays.asList("Urdu","ur","urd")));
      languageIds.put("Vietnamese", Collections.unmodifiableList(
    		  Arrays.asList("Vietnamese","vi","vie")));
      languageIds.put("Algerian Arabic", Collections.unmodifiableList(
    		  Arrays.asList("Algerian Arabic", "arq")));
      languageIds.put("Chinese", Collections.unmodifiableList(
    		  Arrays.asList("Chinese", "zh", "chi", "zho")));
      languageIds.put("Dari", Collections.unmodifiableList(
    		  Arrays.asList("Dari", "prs", "aiq", "haz")));
      languageIds.put("Farsi", Collections.unmodifiableList(
    		  Arrays.asList("Farsi","fas","per")));
      languageIds.put("Kurdish", Collections.unmodifiableList(
    		  Arrays.asList("Kurdish","ku","kur")));
      languageIds.put("Kurdish Sorani", Collections.unmodifiableList(
    		  Arrays.asList("Kurdish Sorani", "ckb")));
      languageIds.put("Pashto", Collections.unmodifiableList(
    		  Arrays.asList("Pashto","Afghani","ps","pus")));
      languageIds.put("Somali", Collections.unmodifiableList(
    		  Arrays.asList("Somali","so","som")));
      languageIds.put("Dutch", Collections.unmodifiableList(
    		  Arrays.asList("Dutch","nl","nld","dut","Flemish","Flamand")));
      languageIds.put("Basque", Collections.unmodifiableList(
    		  Arrays.asList("Basque","baq","eu","eus")));
      languageIds.put("Bosnian", Collections.unmodifiableList(
    		  Arrays.asList("Bosnian","bos","bs","bosniaque")));
      languageIds.put("Danish", Collections.unmodifiableList(
    		  Arrays.asList("Danish","dan","da","danois")));
      languageIds.put("Finnish", Collections.unmodifiableList(
    		  Arrays.asList("Finnish","fi","fin","finnois")));
      languageIds.put("Greek", Collections.unmodifiableList(
    		  Arrays.asList("Greek","el","ell")));
      languageIds.put("Hebrew", Collections.unmodifiableList(
    		  Arrays.asList("Hebrew","heb","he")));
      languageIds.put("Hungarian", Collections.unmodifiableList(
    		  Arrays.asList("Hungarian","hu","hun","hongrois")));
      languageIds.put("Italian", Collections.unmodifiableList(
    		  Arrays.asList("Italian","it","ita","italien")));
      languageIds.put("Norwegian", Collections.unmodifiableList(
    		  Arrays.asList("Norwegian","no","nor")));
      languageIds.put("Polish", Collections.unmodifiableList(
    		  Arrays.asList("Polish","pl","pol")));
      languageIds.put("Romanian", Collections.unmodifiableList(
    		  Arrays.asList("Romanian","ro","rum","ron")));
      languageIds.put("Slovak", Collections.unmodifiableList(
    		  Arrays.asList("Slovak","sk","slo","slk")));
      languageIds.put("Slovenian", Collections.unmodifiableList(
    		  Arrays.asList("Slovenian","sl","slv")));
      languageIds.put("Swedish", Collections.unmodifiableList(
    		  Arrays.asList("Swedish","sv","swe")));
      languageIds.put("Welsh", Collections.unmodifiableList(
    		  Arrays.asList("Welsh","wel","cy","cym")));
      languageIds.put("Serbian", Collections.unmodifiableList(
    		  Arrays.asList("Serbian","sr","srp","serbe")));
      languageIds.put("Turkish", Collections.unmodifiableList(
    		  Arrays.asList("Turkish","tr","tur","turc")));
      

      Map<String,Language> reverseLanguageIds = new HashMap<String,Language>();
      for (String language_string : languageIds.keySet()) {
          Language l = new Language(language_string);
    	  for (String id : languageIds.get(language_string)) {
              reverseLanguageIds.put (id, l);
          }
      }
      REVERSE_LANGUAGE_IDS = Collections.unmodifiableMap (reverseLanguageIds);

      StringBuilder reLanguageIds = new StringBuilder ();
      for (String id : REVERSE_LANGUAGE_IDS.keySet()) {
          reLanguageIds.append (reLanguageIds.length() == 0 ? "(?<![a-zA-Z0-9])(?:" : "|");
          reLanguageIds.append (id);
      }
      reLanguageIds.append (")(?![a-zA-Z0-9])");
      RE_LANGUAGE_IDS = Pattern.compile (reLanguageIds.toString(), Pattern.CASE_INSENSITIVE);
    }

    public static List<LanguageIndependentUrl> getLanguageIndependentUrls (String strUrl) {
        List<LanguageIndependentUrl> ret = new ArrayList<LanguageIndependentUrl>();

      try {
        URI uri = new URI (strUrl);

        for (int i = 0; i < 2; i++) {
          String urlPart = i == 0 ? uri.getPath() : uri.getQuery();
          if (urlPart == null)
            continue;
          Matcher m = RE_LANGUAGE_IDS.matcher (urlPart);
          while (m.find()) {
            ret.add (
              new LanguageIndependentUrl (
                new URI (
                  uri.getScheme(),
                  uri.getUserInfo(),
                  uri.getHost(),
                  uri.getPort(),
                  (i == 0
                   ? uri.getPath().substring(0,m.start()) + "*" + uri.getPath().substring(m.end())
                   : uri.getPath()),
                  (i == 1
                   ? uri.getQuery().substring(0,m.start()) + "*" + uri.getQuery().substring(m.end())
                   : uri.getQuery()),
                  uri.getFragment()
                ).toString(),
                strUrl,
                REVERSE_LANGUAGE_IDS.get (m.group())
              )
            );
          }
        }

      } catch (URISyntaxException ex) {
          // System.err.println ("Can't parse URL: " + ex.getInput());
      }

      return ret;
    }

    public void map(Text key, ArcRecord value, OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {

      try {
        if (!value.getContentType().contains("html")) {
          reporter.incrCounter(this._counterGroup, "Skipped - Not HTML", 1);
          return;
        }
        /*
        if (value.getContentLength() > 1000000) {
        	reporter.incrCounter(this._counterGroup, "Skipped - Too Long", 1);
        	return;
        }*/

        String url = value.getURL();
        List<LanguageIndependentUrl> lius = getLanguageIndependentUrls(url);
        for (LanguageIndependentUrl liu : lius) {
          output.collect(new Text(liu.urlBase), new Text(liu.fullUrl.toString()));
        }
      }
      catch (Throwable e) {

        // occassionally Jsoup parser runs out of memory ...
        if (e.getClass().equals(OutOfMemoryError.class))
          System.gc();

        LOG.error("Caught Exception", e);
        reporter.incrCounter(this._counterGroup,
            "Skipped - Exception Thrown (mapper)", 1);
      }
    }
  }
  public static class URLMinerReducer
      extends    MapReduceBase
      implements Reducer<Text, Text, Text, Text> {
	
	private final String _counterGroup = "Custom Reducer Counters";
    
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) 
        throws IOException {

      try {
        StringWriter out = new StringWriter();
        int count = 0;
        while (values.hasNext()) {
          if (count > 0) {
        	out.append("\t");
          }
          out.append(values.next().toString());
          count++;
        }
        output.collect(key, new Text(out.toString()));
          
      } catch (Throwable e) {
        // occassionally Jsoup parser runs out of memory ...
        if (e.getClass().equals(OutOfMemoryError.class))
          System.gc();

        LOG.error("Caught Exception", e);
      }
    }
    private String ExtractHtml(ArcRecord rec) throws IOException {
      try {
        String result = CCParc.getHtmlStr(rec);
        result = result.replace("\t", "\\t");
        result = result.replace("\n", "\\n");
        return result;
      } catch (SkippedDocument ex) {
        return "";
      }
    }
  }

  /**
   * Hadoop FileSystem PathFilter for ARC files, allowing users to limit the
   * number of files processed.
   *
   * @author Chris Stephens <chris@commoncrawl.org>
   */
  public static class SampleFilter
      implements PathFilter {

    private static int count =         0;
    private static int max   = 999999999;

    public boolean accept(Path path) {

      if (!path.getName().endsWith(".arc.gz"))
        return false;

      SampleFilter.count++;

      if (SampleFilter.count > SampleFilter.max)
        return false;

      return true;
    }
  }

  /**
   * Implmentation of Tool.run() method, which builds and runs the Hadoop job.
   *
   * @param  args command line parameters, less common Hadoop job parameters stripped
   *              out and interpreted by the Tool class.  
   * @return      0 if the Hadoop job completes successfully, 1 if not. 
   */
  @Override
  public int run(String[] args)
      throws Exception {

    String inputPath = null;
    String outputPath = null;
    String configFile = null;

    // Read the command line arguments.
    if (args.length <  2)
      throw new IllegalArgumentException("Example JAR must be passed input and output paths.");

    inputPath = args[0];
    outputPath = args[1];

    if (args.length >= 3)
      configFile = args[2];

    // Read in any additional config parameters.
    if (configFile != null) {
      LOG.info("adding config parameters from '"+ configFile + "'");
      this.getConf().addResource(configFile);
    }

    // Creates a new job configuration for this Hadoop job.
    JobConf job = new JobConf(this.getConf());

    job.setJarByClass(URLMiner.class);

    // Scan the provided input path for ARC files.
    LOG.info("setting input path to '"+ inputPath + "'");
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileInputFormat.setInputPathFilter(job, SampleFilter.class);

    // Delete the output path directory if it already exists.
    LOG.info("clearing the output path at '" + outputPath + "'");

    FileSystem fs = FileSystem.get(new URI(outputPath), job);

    if (fs.exists(new Path(outputPath)))
      fs.delete(new Path(outputPath), true);

    // Set the path where final output 'part' files will be saved.
    LOG.info("setting output path to '" + outputPath + "'");
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    // Set which InputFormat class to use.
    job.setInputFormat(ArcInputFormat.class);

    // Set which OutputFormat class to use.
    job.setOutputFormat(TextOutputFormat.class);
    TextOutputFormat.setCompressOutput(job, true);
    TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    // Set the output data types.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Set which Mapper and Reducer classes to use.
    job.setMapperClass(URLMiner.URLMinerMapper.class);
    job.setReducerClass(URLMiner.URLMinerReducer.class);

    if (JobClient.runJob(job).isSuccessful())
      return 0;
    else
      return 1;
  }

  /**
   * Main entry point that uses the {@link ToolRunner} class to run the example
   * Hadoop job.
   */
  public static void main(String[] args)
      throws Exception {
    int res = ToolRunner.run(new Configuration(), new URLMiner(), args);
    System.exit(res);
  }
}

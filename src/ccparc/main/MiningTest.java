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
public class MiningTest
    extends    Configured
    implements Tool {

  private static final Logger LOG = Logger.getLogger(MiningTest.class);

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
  public static class MiningTestMapper
      extends    MapReduceBase
      implements Mapper<Text, ArcRecord, Text, MiningRecord> {
 
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
      // Already done for WMT
      /*
      languageIds.put("Spanish", Collections.unmodifiableList(
    		  Arrays.asList ("es", "esp", "spa", "spanish", "espanol")));
      languageIds.put("French", Collections.unmodifiableList (
    		  Arrays.asList ("fr", "fre", "fra", "french")));
      languageIds.put("German", Collections.unmodifiableList (
    		  Arrays.asList ("de", "deu", "ger", "german", "deutsch")));
      languageIds.put("Russian", Collections.unmodifiableList(
    		  Arrays.asList("Russian","ru","rus")));
      languageIds.put("Czech", Collections.unmodifiableList(
    		  Arrays.asList("Czech","cs","ces","cze")));
      */
      
      languageIds.put("Korean", Collections.unmodifiableList(
    		  Arrays.asList("Korean","ko","kor")));      
      languageIds.put("Arabic", Collections.unmodifiableList(
    		  Arrays.asList("Arabic","ar","ara")));
      languageIds.put("Bengali", Collections.unmodifiableList(
    		  Arrays.asList("Bengali","bn","ben")));
      languageIds.put("Bulgarian", Collections.unmodifiableList(
    		  Arrays.asList("Bulgarian","bg","bul")));
      languageIds.put("Japanese", Collections.unmodifiableList(
    		  Arrays.asList("Japanese","ja","jpn")));
      languageIds.put("Kannada", Collections.unmodifiableList(
    		  Arrays.asList("Kannada","kn","kan")));
      languageIds.put("Tamil", Collections.unmodifiableList(
    		  Arrays.asList("Tamil","ta","tam")));
      languageIds.put("Telugu", Collections.unmodifiableList(
    		  Arrays.asList("Telugu","te","tel")));
      languageIds.put("Urdu", Collections.unmodifiableList(
    		  Arrays.asList("Urdu","ur","urd")));
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
      // Avoiding these
      /*
//      languageIds.put("Indonesian", Collections.unmodifiableList(
//    		  Arrays.asList("Indonesian","Bahasa Indonesia","id","ind")));
      languageIds.put("Vietnamese", Collections.unmodifiableList(
    		  Arrays.asList("Vietnamese","vi","vie")));
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
      languageIds.put("Romanian", Collections.unmodifiableList(
    		  Arrays.asList("Romanian","ro","rum","ron")));
      languageIds.put("Slovak", Collections.unmodifiableList(
    		  Arrays.asList("Slovak","sk","slo","slk")));
      languageIds.put("Slovenian", Collections.unmodifiableList(
    		  Arrays.asList("Slovenian","sl","slv")));
      languageIds.put("Swedish", Collections.unmodifiableList(
    		  Arrays.asList("Swedish","sv","swe")));
      languageIds.put("Turkish", Collections.unmodifiableList(
    		  Arrays.asList("Turkish","tr","tur","turc")));
      languageIds.put("Polish", Collections.unmodifiableList(
    		  Arrays.asList("Polish","pl","pol")));
      languageIds.put("Italian", Collections.unmodifiableList(
    		  Arrays.asList("Italian","it","ita","italien")));
      languageIds.put("Norwegian", Collections.unmodifiableList(
    		  Arrays.asList("Norwegian","no","nor")));
      languageIds.put("Welsh", Collections.unmodifiableList(
    		  Arrays.asList("Welsh","wel","cy","cym")));
      languageIds.put("Serbian", Collections.unmodifiableList(
    		  Arrays.asList("Serbian","sr","srp","serbe")));
      */
      

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

    public void map(Text key, ArcRecord value, OutputCollector<Text, MiningRecord> output, Reporter reporter)
        throws IOException {

      try {
        if (!value.getContentType().contains("html")) {
          //reporter.incrCounter(this._counterGroup, "Skipped - Not HTML", 1);
          return;
        }
        
        if (value.getContentLength() > 1000000) {
          //reporter.incrCounter(this._counterGroup, "Skipped - Too Long", 1);
          return;
        }

        String url = value.getURL();
        List<LanguageIndependentUrl> lius = getLanguageIndependentUrls(url);
        for (LanguageIndependentUrl liu : lius) {
          output.collect(new Text(liu.urlBase), new MiningRecord(liu, value));
        }
      }
      catch (Throwable e) {

        // occassionally Jsoup parser runs out of memory ...
        if (e.getClass().equals(OutOfMemoryError.class))
          System.gc();

        LOG.error("Caught Exception", e);
        /*
        reporter.incrCounter(this._counterGroup,
            "Skipped - Exception Thrown (mapper)", 1);
            */
      }
    }
  }
  public static class MiningTestReducer
      extends    MapReduceBase
      implements Reducer<Text, MiningRecord, Text, Text> {
	
	private final String _counterGroup = "Custom Reducer Counters";
    
	public void reduce(Text key, Iterator<MiningRecord> values, OutputCollector<Text, Text> output, Reporter reporter) 
        throws IOException {

      try {
        int count = 0;
        String first_lang = "";
        boolean two_langs = false;
        
        MiningRecord first_record = new MiningRecord();
        ArrayList<String> fields = new ArrayList<String>();
        // Avoid creating any output unless there are at least two values
        // TODO: I'm picking up English/English pairs here
        int total_size = 0;
        while (values.hasNext()) {
          if (count == 0) {
            first_record = values.next();
            total_size += first_record.arc_record.getContentLength();
            first_lang = first_record.liu.lang.toString();
          } else if (count == 1) {
            fields.add(first_record.liu.lang.toString());
            fields.add(first_record.liu.fullUrl.toString());
            String first_html = ExtractHtml(first_record.arc_record);
            fields.add(first_html);
          }
          if (count >= 1) {
            MiningRecord current_rec = values.next();
            total_size += current_rec.arc_record.getContentLength();
            if (current_rec.liu.lang.toString() != first_lang) {
            	two_langs = true;
            }
            fields.add(current_rec.liu.lang.toString());
            fields.add(current_rec.liu.fullUrl.toString());
            String current_html = ExtractHtml(current_rec.arc_record);
            fields.add(current_html);
          }
          count++;
        }
        if ((count > 1) && (two_langs == true)) {
        	/*
          reporter.incrCounter(this._counterGroup, "Matches", 1);
          reporter.incrCounter(this._counterGroup, "Match data", total_size);
          */
          StringWriter out = new StringWriter();
          
          for (int i = 0; i < fields.size(); i++) {
            out.append(fields.get(i));
            if (i != fields.size() - 1) {
              out.append("\t");
            }
          }
          output.collect(key, new Text(out.toString()));
        } else {
          /*
          StringWriter out = new StringWriter();
          out.append(first_record.liu.fullUrl.toString());
          output.collect(key, new Text(out.toString()));
          reporter.incrCounter(this._counterGroup, "Misses", 1);
          reporter.incrCounter(this._counterGroup, "Miss data", total_size);
          */
        }
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

    job.setJarByClass(MiningTest.class);

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
    job.setOutputValueClass(MiningRecord.class);

    // Set which Mapper and Reducer classes to use.
    job.setMapperClass(MiningTest.MiningTestMapper.class);
    job.setReducerClass(MiningTest.MiningTestReducer.class);

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
    int res = ToolRunner.run(new Configuration(), new MiningTest(), args);
    System.exit(res);
  }
}

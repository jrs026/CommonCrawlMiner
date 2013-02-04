// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------
// includes

package ccparc.hadoop;

import ccparc.structs.Candidate;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.commoncrawl.hadoop.mapred.ArcInputFormat;

//---------------------------------------------------------------------------------------------------------------------------------

public class CCParcHadoopMain
    extends Configured
    implements Tool
{

    public static class ArcFileFilter implements PathFilter {
        public boolean accept (Path path) {
            return path.getName().endsWith (".arc.gz");
        }
    }

    public int run (String[] args)
        throws Exception
    {
        if (args.length != 2) {
            System.err.println ("usage: hadoop CCParcHadoopMain <input-dir> <output-dir>");
            System.exit (-1);
        }

        JobConf job = new JobConf (getConf());
        job.setJarByClass (CCParcHadoopMain.class);
        job.setJobName ("CCParc");

        FileInputFormat.addInputPath (job, new Path (args[0]));
        FileInputFormat.setInputPathFilter (job, ArcFileFilter.class);
        FileOutputFormat.setOutputPath (job, new Path (args[1]));

        job.setMapperClass (CCParcMapper.class);
        job.setReducerClass (CCParcReducer.class);

        job.setOutputKeyClass (Text.class);
        job.setOutputValueClass (Text.class);

        job.setMapOutputKeyClass (Text.class);
        job.setMapOutputValueClass (Candidate.class);

        job.setInputFormat (ArcInputFormat.class);
        job.setOutputFormat (TextOutputFormat.class);

        return JobClient.runJob(job).isSuccessful() ? 0 : 1;
    }

    public static void main (String[] args)
        throws Exception
    {
        System.exit (ToolRunner.run (new CCParcHadoopMain(), args));
    }

}

//---------------------------------------------------------------------------------------------------------------------------------

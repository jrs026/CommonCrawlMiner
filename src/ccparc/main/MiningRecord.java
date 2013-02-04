package ccparc.main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import org.commoncrawl.hadoop.mapred.ArcRecord;

import ccparc.structs.LanguageIndependentUrl;
import ccparc.structs.Language;

public class MiningRecord implements Writable {
  public LanguageIndependentUrl liu;
  public ArcRecord arc_record;

  public MiningRecord() {
    liu = new LanguageIndependentUrl("","",new Language(""));
    arc_record = new ArcRecord();
  }
  public MiningRecord(LanguageIndependentUrl l, ArcRecord r) {
    liu = l;
    arc_record = r;
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(liu.urlBase.toString());
    out.writeUTF(liu.lang.toString());
    out.writeUTF(liu.fullUrl.toString());
    arc_record.write(out);
  }
  
  public void readFields(DataInput in) throws IOException {
    String urlBase = in.readUTF();
    String lang = in.readUTF();
    String fullUrl = in.readUTF();
    arc_record.readFields(in);
    liu = new LanguageIndependentUrl(urlBase, fullUrl, new Language(lang));
  }
}

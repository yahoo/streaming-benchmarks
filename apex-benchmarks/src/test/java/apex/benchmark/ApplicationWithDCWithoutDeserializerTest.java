package apex.benchmark;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

public class ApplicationWithDCWithoutDeserializerTest extends ApplicationWithDCWithoutDeserializer
{
  public ApplicationWithDCWithoutDeserializerTest()
  {
    includeQuery = false;
    includeRedisJoin = false;
  }

  @Test
  public void test() throws Exception
  {
    Configuration conf = new Configuration(false);
    conf.set(PROP_STORE_PATH, "target/tmp");

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    super.populateDAG(dag, conf);

    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.run(600000);

    lc.shutdown();
  }

}

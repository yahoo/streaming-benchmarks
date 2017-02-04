/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

@Stateless
public class FilterFields extends BaseOperator
{
  public transient DefaultInputPort<JSONObject> input = new DefaultInputPort<JSONObject>()
  {
    @Override
    public void process(JSONObject jsonObject)
    {
      try {
        Tuple tuple = new Tuple();
        tuple.adId = jsonObject.getString("ad_id");
        tuple.event_time = jsonObject.getString("event_time");

        output.emit(tuple);
      } catch (JSONException e) {
        throw new RuntimeException(e);
      }
    }

  };

  public transient DefaultOutputPort<Tuple> output = new DefaultOutputPort();
}


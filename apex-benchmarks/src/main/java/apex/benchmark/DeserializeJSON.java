/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;

public class DeserializeJSON extends BaseOperator
{
  public transient DefaultInputPort<String> input = new DefaultInputPort<String>()
    {
      @Override
      public void process(String t)
      {
        JSONObject jsonObject;
        try {
          jsonObject = new JSONObject(t);
        } catch (JSONException e) {
          throw DTThrowable.wrapIfChecked(e);
        }

        output.emit(jsonObject);
      }
  };

  public transient DefaultOutputPort<JSONObject> output = new DefaultOutputPort();
}


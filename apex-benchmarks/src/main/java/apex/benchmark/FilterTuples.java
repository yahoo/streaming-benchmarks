/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;

@Stateless
public class FilterTuples extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(FilterTuples.class);

  public transient DefaultInputPort<JSONObject> input = new DefaultInputPort<JSONObject>()
    {
      @Override
      public void process(JSONObject jsonObject)
      {
        try {
          if (  jsonObject.getString("event_type").equals("view") ) {
            output.emit(jsonObject);
          }
        } catch (JSONException e) {
          DTThrowable.wrapIfChecked(e);
        }
      }
  };

  public transient DefaultOutputPort<JSONObject> output = new DefaultOutputPort();
}


/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.example.pe.example;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.model.schema.PropertyScope;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.helpers.*;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.vocabulary.SO;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.standalone.ProcessorParams;
import org.apache.streampipes.wrapper.standalone.StreamPipesDataProcessor;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;


public class PowerTrackingDWM extends StreamPipesDataProcessor {

  private String input_power_value;
  private String input_timestamp_value;
  private int day_precedent = 0, month_precedent = 0, range = 0;
  double daily_consumption = 0.0;
  double monthly_consumption = 0.0;
  double seven_day_consumption = 0.0;
  private static final String INPUT_VALUE = "value";
  private static final String TIMESTAMP_VALUE = "timestamp_value";
  private static final String DAILY_CONSUMPTION = "daily_consumption";
  private static final String SEVENDAY_CONSUMPTION = "seven_day_consumption";
  private static final String MONTHLY_CONSUMPTION = "monthly_consumption";

  List<Double> powersList = new ArrayList<>();
  List<Long> timestampsList = new ArrayList<>();
  List<Double> dailyConsumptionListForMonth = new ArrayList<>();
  List<Double> dailyConsumptionListForSevenDay = new ArrayList<>();

  @Override
  public DataProcessorDescription declareModel() {
    return ProcessingElementBuilder.create("org.example.pe.example.processor","PowerTrackingDWM", "Computes Daily and Monthly Energy Consumption based on the given instantaneous powers and timestamps values.")
            .withAssets(Assets.DOCUMENTATION, Assets.ICON)
            .withLocales(Locales.EN)
            .category(DataProcessorType.AGGREGATE)
            .requiredStream(StreamRequirementsBuilder.create()
                    .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                            Labels.withId(INPUT_VALUE), PropertyScope.NONE)
                    .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                            Labels.withId(TIMESTAMP_VALUE), PropertyScope.NONE)
                    .build())
            .outputStrategy(OutputStrategies.append(EpProperties.doubleEp(Labels.withId(MONTHLY_CONSUMPTION), "monthly consumption", SO.Number),
                    EpProperties.doubleEp(Labels.withId(DAILY_CONSUMPTION), "daily consumption", SO.Number),
                    EpProperties.doubleEp(Labels.withId(SEVENDAY_CONSUMPTION), "seven day consumption", SO.Number)))
            .build();
  }


  @Override
  public void onInvocation(ProcessorParams parameters, SpOutputCollector out, EventProcessorRuntimeContext ctx) throws SpRuntimeException  {
    this.input_power_value = parameters.extractor().mappingPropertyValue(INPUT_VALUE);
    this.input_timestamp_value = parameters.extractor().mappingPropertyValue(TIMESTAMP_VALUE);
  }


  @Override
  public void onEvent(Event event,SpOutputCollector out){
    Double power;
    Long timestamp;
    int day_current, month_current;

    //recovery power value
    power = event.getFieldBySelector(this.input_power_value).getAsPrimitive().getAsDouble();

    //recovery timestamp value
    timestamp = event.getFieldBySelector(this.input_timestamp_value).getAsPrimitive().getAsLong();

    day_current = getDayOrMonth(timestamp, 'd');
    month_current = getDayOrMonth(timestamp, 'm');

    if((day_current != day_precedent || month_current != month_precedent) && day_precedent != 0){

      if(day_current == day_precedent){
        range = range + 1;
        //perform operations to obtain hourly power from instantaneous powers
        daily_consumption = powersToEnergyConsumption(powersList, timestampsList);
        logger.info("=============== OUTPUT DAILY CONSUMPTION  =========" + daily_consumption  + " kWh" + timestamp);
        dailyConsumptionListForSevenDay.add(daily_consumption);
        dailyConsumptionListForMonth.add(daily_consumption);
        // Remove all elements from the Lists
        clearLists(powersList, timestampsList);
        // Add current events for the next computation
        addToLists(power, timestamp);
      }

      if(day_current != day_precedent){
        range = range + 1;
        // reset day for computations
        day_precedent = day_current;
        //perform operations to obtain hourly power from instantaneous powers
        daily_consumption = powersToEnergyConsumption(powersList, timestampsList);
        logger.info("=============== OUTPUT DAILY CONSUMPTION  =========" + daily_consumption + " kWh" + timestamp);
        dailyConsumptionListForSevenDay.add(daily_consumption);
        dailyConsumptionListForMonth.add(daily_consumption);
        // Remove all elements from the Lists
        clearLists(powersList, timestampsList);
        // Add current events for the next computation
        addToLists(power, timestamp);
      }

      if(range == 7){
        range = 0;
        seven_day_consumption = dailyConsumptionsToMonthlyConsumption(dailyConsumptionListForSevenDay);
        logger.info("=============== OUTPUT SEVEN DAY CONSUMPTION  =========" + seven_day_consumption + " kWh");
        dailyConsumptionListForSevenDay.clear();
      }

      if(month_current != month_precedent){
        month_precedent = month_current;
        monthly_consumption = dailyConsumptionsToMonthlyConsumption(dailyConsumptionListForMonth);
        logger.info("=============== OUTPUT MONTHLY CONSUMPTION  =========" + monthly_consumption + " kWh");
        dailyConsumptionListForMonth.clear();
      }

    }else {
      // set the start time for computations
      if (day_precedent == 0){
        month_precedent = month_current;
        day_precedent = day_current;
      }
      // add power to the lists
      addToLists(power, timestamp);
    }

    event.addField("daily consumption", daily_consumption);
    event.addField("seven day consumption", seven_day_consumption);
    event.addField("monthly consumption", monthly_consumption);

    out.collect(event);

  }

  private int getDayOrMonth(Long timestamp, char c) {
    // Convert the timestamp to an Instant
    //Instant instant = Instant.ofEpochMilli(timestamp);
    Date instant = new Date(timestamp);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    String dateString = sdf.format(instant);
    // Use the Instant to create a Date object
    String[] date = dateString.split(" ");
    String[] ymd = date[0].split("-");
    int day = Integer.parseInt(ymd[2]);
    int month = Integer.parseInt(ymd[1]);
    return c=='d' ? day:month;
  }

  private void addToLists(Double power, Long timestamp) {
    powersList.add(power);
    timestampsList.add(timestamp);
  }

  private void clearLists(List<Double> powersList, List<Long> timestampsList) {
    powersList.clear();
    timestampsList.clear();
  }

  private double dailyConsumptionsToMonthlyConsumption(List<Double> dailyConsumptionList) {
    double sum = 0.0;
    for (Double value : dailyConsumptionList) sum = sum + value;
    return sum;
  }

  public double powersToEnergyConsumption(List<Double> powers, List<Long> timestamps) {
    double sum = 0.0;
    double first_base;
    double second_base;
    long height;
    DecimalFormat df = new DecimalFormat("#.#####");
    df.setRoundingMode(RoundingMode.CEILING);
    //perform Riemann approximations by trapezoids which is an approximation of the area
    // under the curve (which corresponds to the energy/hourly power) formed by the points
    // with coordinate power(ordinate) e timestamp(abscissa)
    for(int i = 0; i<powers.size()-1; i++){
      first_base = powers.get(i);
      second_base = powers.get(i+1);
      height = (timestamps.get(i+1) - timestamps.get(i))/1000;
      sum += ((first_base + second_base) / 2) * height ;
    }
    return Double.parseDouble(df.format(sum/3600/1000));
  }

  @Override
  public void onDetach(){
  }

}
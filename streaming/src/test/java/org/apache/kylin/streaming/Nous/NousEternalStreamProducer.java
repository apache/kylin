package org.apache.kylin.streaming.nous;

import java.util.Calendar;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.streaming.EternalStreamProducer;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * Created by Hongbin Ma(Binmahone) on 3/16/15.
 */
public class NousEternalStreamProducer extends EternalStreamProducer {

    /**
     * @param frequency records added per second, 100 for recommendation
     */
    public NousEternalStreamProducer(int frequency) {
        super(frequency);
    }

    @Override
    protected String getOneMessage() {

        Calendar currentTime = Calendar.getInstance();
        Calendar minuteStart = Calendar.getInstance();
        Calendar hourStart = Calendar.getInstance();

        currentTime.setTimeInMillis(System.currentTimeMillis());
        minuteStart.clear();
        hourStart.clear();

        minuteStart.set(currentTime.get(Calendar.YEAR), currentTime.get(Calendar.MONTH), currentTime.get(Calendar.DAY_OF_MONTH), currentTime.get(Calendar.HOUR_OF_DAY), currentTime.get(Calendar.MINUTE));
        hourStart.set(currentTime.get(Calendar.YEAR), currentTime.get(Calendar.MONTH), currentTime.get(Calendar.DAY_OF_MONTH), currentTime.get(Calendar.HOUR_OF_DAY), 0);

        Random r = new Random();
        NousMessage temp = new NousMessage(minuteStart.getTimeInMillis(), hourStart.getTimeInMillis(), RandomStringUtils.randomAlphabetic(1), RandomStringUtils.randomAlphabetic(1), RandomStringUtils.randomAlphabetic(1), RandomStringUtils.randomAlphabetic(1), RandomStringUtils.randomAlphabetic(1), r.nextInt(5), r.nextDouble() * 100, r.nextInt(2));
        try {
            return JsonUtil.writeValueAsIndentString(temp);
        } catch (JsonProcessingException e) {
            return "";
        }
    }
}

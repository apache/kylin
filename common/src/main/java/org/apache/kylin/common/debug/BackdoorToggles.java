package org.apache.kylin.common.debug;

import java.util.Map;

/**
 */
public class BackdoorToggles {

    private static final ThreadLocal<Map<String, String>> _backdoorToggles = new ThreadLocal();

    public static void setToggles(Map<String, String> toggles) {
        _backdoorToggles.set(toggles);
    }

    public static String getToggle(String key) {
        Map<String, String> toggles = _backdoorToggles.get();
        if (toggles == null) {
            return null;
        } else {
            return toggles.get(key);
        }
    }

    public static void cleanToggles() {
        _backdoorToggles.remove();
    }

    /**
     * set DEBUG_TOGGLE_DISABLE_FUZZY_KEY=true to disable fuzzy key for debug/profile usage
     *
     *
     *
     example:

     "backdoorToggles": {
     "DEBUG_TOGGLE_DISABLE_FUZZY_KEY": "true"
     }

     */
    public final static String DEBUG_TOGGLE_DISABLE_FUZZY_KEY = "DEBUG_TOGGLE_DISABLE_FUZZY_KEY";

    /**
     * set DEBUG_TOGGLE_OBSERVER_BEHAVIOR=SCAN/SCAN_FILTER/SCAN_FILTER_AGGR to control observer behavior for debug/profile usage
     *
     example:

     "backdoorToggles": {
     "DEBUG_TOGGLE_OBSERVER_BEHAVIOR": "SCAN"
     }

     */
    public final static String DEBUG_TOGGLE_OBSERVER_BEHAVIOR = "DEBUG_TOGGLE_OBSERVER_BEHAVIOR";
}

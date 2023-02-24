package Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** GeoUtils provides utility methods to deal with locations for the data streaming exercises. */
public class GeoUtils {

    // geo boundaries of the area of NYC
    public static final double LON_EAST = -73.7;
    public static final double LON_WEST = -74.05;
    public static final double LAT_NORTH = 41.0;
    public static final double LAT_SOUTH = 40.5;

    /**
     * Checks if a location specified by longitude and latitude values is within the geo boundaries
     * of New York City.
     *
     * @param lon longitude of the location to check
     * @param lat latitude of the location to check
     * @return true if the location is within NYC boundaries, otherwise false.
     */
    public static boolean isInNYC(float lon, float lat) {

        return !(lon > LON_EAST || lon < LON_WEST) && !(lat > LAT_NORTH || lat < LAT_SOUTH);
    }
}
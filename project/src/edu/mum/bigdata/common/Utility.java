package edu.mum.bigdata.common;

public class Utility
{
    public static int toNumber(String strNum) {
        int d = 0;
        try {
            d = Integer.parseInt(strNum);
        } catch (NumberFormatException | NullPointerException nfe) {
            return 0;
        }
        return d;
    }
}

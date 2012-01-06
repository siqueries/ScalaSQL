package com.solab.sql;

import java.math.BigDecimal;

/**
 * Contains a couple of functions that are registered as SP's in H2, for testing purposes.
 *
 * @author Enrique Zamudio
 *         Date: 06/01/12 15:24
 */
public class H2Functions {

    public static String function1() {
        return "This is function1";
    }

    public static BigDecimal function2(String p1, BigDecimal p2) {
        System.out.printf("Called from H2: %s%n", p1);
        return p2.add(BigDecimal.ONE);
    }

}

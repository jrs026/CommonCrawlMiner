// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.util;

//---------------------------------------------------------------------------------------------------------------------------------

public class IoUtils {

    /**
     * Simply calls System.out.println. Two advantages: 1- can be static-imported, for shorter calls 2- synchronized
     */
    public static void println (Object arg) {
        synchronized (System.out) {
            System.out.println (arg);
        }
    }

}

//---------------------------------------------------------------------------------------------------------------------------------

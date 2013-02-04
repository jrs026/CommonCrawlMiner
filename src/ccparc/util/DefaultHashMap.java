// $Id$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.util;

import java.util.Map;
import java.util.HashMap;

//---------------------------------------------------------------------------------------------------------------------------------

/**
 * A HashMap that automatically inserts and returns a default value when you call `get' on a key that was not contained in the map.
 * Inspired by Python's `defaultdict'.
 */

public abstract class DefaultHashMap<K,V>
    extends HashMap<K,V>
{

    //-----------------------------------------------------------------------------------------------------------------------------
    // constructors are the same as HashMap

    public DefaultHashMap () {
        super ();
    }

    public DefaultHashMap (int initialCapacity) {
        super (initialCapacity);
    }

    public DefaultHashMap (int initialCapacity, float loadFactor) {
        super (initialCapacity, loadFactor);
    }

    public DefaultHashMap (Map<? extends K,? extends V> m) {
        super(m);
    }

    //-----------------------------------------------------------------------------------------------------------------------------
    // The one abstract method you need to override

    protected abstract V defaultValue ();

    //-----------------------------------------------------------------------------------------------------------------------------
    // overriden methods

    @SuppressWarnings("unchecked") // sigh
    public V get (Object key) {
        V val = super.get (key);

        // NB we assume, perhaps wrongly, that in most cases when this method is called there will be a value to return. So we wait
        // until we get a null before calling `containsKey', to save a few CPU cycles.
        if (val == null && !containsKey(key)) {
            val = defaultValue();
            put ((K) key, val);
        }

        return val;
    }

    //-----------------------------------------------------------------------------------------------------------------------------


}

//---------------------------------------------------------------------------------------------------------------------------------

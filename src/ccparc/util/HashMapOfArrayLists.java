// $Id$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.util;

import java.util.ArrayList;
import java.util.Map;

//---------------------------------------------------------------------------------------------------------------------------------

/**
 * A HashMap of ArrayLists that automatically creates a new list when you `get' a new key.
 */

public class HashMapOfArrayLists<K,V>
    extends DefaultHashMap<K,ArrayList<V>>
{

    public static final long serialVersionUID = 1L;

    public HashMapOfArrayLists () {
        super ();
    }

    public HashMapOfArrayLists (int initialCapacity) {
        super (initialCapacity);
    }

    public HashMapOfArrayLists (int initialCapacity, float loadFactor) {
        super (initialCapacity, loadFactor);
    }

    protected ArrayList<V> defaultValue () {
        // TODO make the initial capacity configurable? In any case you probably want to keep this low because these structs
        // typically use loads of memory.
        return new ArrayList<V> (1);
    }

    public void add (K key, V val) {
        get(key).add(val);
    }

    public void addAll (HashMapOfArrayLists<K,V> other) {
        for (Map.Entry<K,ArrayList<V>> entry : other.entrySet())
            get(entry.getKey()).addAll (entry.getValue());
    }

}

//---------------------------------------------------------------------------------------------------------------------------------

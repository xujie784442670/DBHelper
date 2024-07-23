using System.Collections.Generic;

namespace Helper;

public static class Extension
{
    public static void AddAll<K, V>(this Dictionary<K, V> dict, Dictionary<K, V> values)
    {
        if (values == null) return;
        foreach (var keyValuePair in values)
        {
            dict[keyValuePair.Key] = keyValuePair.Value;
        }
    }
}
namespace Net.Code.Kbo;
static class LinqEx
{
    public static IEnumerable<T[]> Batch<T>(
            this IEnumerable<T> source, int size)
    {
        T[] buffer = new T[size];
        var count = 0;

        foreach (var item in source)
        {
            buffer[count++] = item;

            if (count != size)
                continue;
            yield return buffer;

            count = 0;
        }
        if (buffer != null && count > 0)
        {
            Array.Resize(ref buffer, count);
            yield return buffer;
        }
    }
}
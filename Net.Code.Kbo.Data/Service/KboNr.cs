using System.ComponentModel;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Net.Code.Kbo;

[TypeConverter(typeof(KboNrConverter))]
public record struct KboNr(long Value) : IFormattable
{
    internal sealed class KboNrConverter : TypeConverter
    {
        public override bool CanConvertFrom(ITypeDescriptorContext? context, Type sourceType)
            => sourceType == typeof(string) || base.CanConvertFrom(context, sourceType);
        public override bool CanConvertTo(ITypeDescriptorContext? context, Type? destinationType)
            => destinationType == typeof(string) || base.CanConvertTo(context, destinationType);
        public override object? ConvertFrom(ITypeDescriptorContext? context, CultureInfo? culture, object value)
            => value is string v && TryParse(v, out var kbonr) ? kbonr : base.ConvertFrom(context, culture, value);
        public override object? ConvertTo(ITypeDescriptorContext? context, CultureInfo? culture, object? value, Type destinationType)
            => destinationType == typeof(string) && value is KboNr kbonr
                ? kbonr.ToString("F", CultureInfo.InvariantCulture)
                : base.ConvertTo(context, culture, value, destinationType);
    }

    public static bool TryParse(string? s, out KboNr kbonr)
    {
        kbonr = default;
        if (string.IsNullOrEmpty(s)) return false;
        if (s.Count(char.IsDigit) != 10) return false;
        long num = ParseToLong(s);
        if (IsValid(num))
        {
            kbonr = new KboNr(num);
            return true;
        }

        kbonr = default;
        return false;
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsValid(long value)
    {
        long num = Math.DivRem(value, 1000000L, out var result);
        return num < 10000;
    }

    public static KboNr Parse(string s)
    {
        if (TryParse(s, out var kbonr))
        {
            return kbonr;
        }
        throw new FormatException("The supplied value is not a valid KboNr.");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsValid(string value) => TryParse(value, out _);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsValid() => IsValid(Value);

    public override string ToString() => ToString("N", CultureInfo.InvariantCulture);
    public string ToString(string format) => ToString(format, CultureInfo.CurrentCulture);

    public string ToString(string? format, IFormatProvider? formatProvider)
    {
        if (Value is 0)
        {
            return string.Empty;
        }


        switch (format)
        {
            case "F":
            {
                var (part1, part2, part3) = Components(Value);
                return $"{part1:0000}.{part2:000}.{part3:000}";
            }
            default:
                return Value.ToString("0000000000", formatProvider);
        }
        static (long part1, long part2, long part3) Components(long value)
        {
            long a = value;
            a = Math.DivRem(a, 1000L, out var part3);
            a = Math.DivRem(a, 1000L, out var part2);
            long part1 = a;
            return (part1, part2, part3);
        }
    }

    private static long ParseToLong(string s)
    {
        long num = 0L;
        foreach (char c in s)
        {
            if (char.IsDigit(c))
            {
                num = 10 * num + (c - 48);
            }
        }

        return num;
    }

}

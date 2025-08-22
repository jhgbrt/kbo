namespace Net.Code.Kbo;

static class Extensions
{
    extension(TimeSpan span)
    {
        public string ToShortString() => span switch
        {
            { TotalDays: > 1 } => $"{(int)span.TotalDays}d {span.Hours}h {span.Minutes}m",
            { TotalDays: 1 } => $"1d",
            { TotalHours: > 1 } => $"{span.Hours}h {span.Minutes}m {span.Seconds}s",
            { TotalHours: 1 } => $"1h",
            { TotalMinutes: > 1 } => $"{span.Minutes}m {span.Seconds}s",
            { TotalMinutes: 1 } => $"1m",
            { TotalSeconds: > 1 } => $"{span.Seconds}s {span.Milliseconds} ms",
            { TotalSeconds: 1 } => $"1s",
            { TotalMilliseconds: > 1 } => $"{span.TotalMilliseconds:0.00} ms",
            { TotalMilliseconds: 1 } => $"1 ms",
            { TotalMicroseconds: > 1 } => $"{span.TotalMicroseconds:0.00} µs",
            { TotalMicroseconds: 1 } => $"1 µs",
            _ => $"{span.TotalNanoseconds} ns"
        };
    }
}
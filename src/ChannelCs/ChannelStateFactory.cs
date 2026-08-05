namespace ChannelCs;

public static class ChannelStateFactory
{
    public static TState CreateDefault<TState>()
    {
        if (typeof(TState).IsValueType)
        {
            return default!;
        }

        try
        {
            return Activator.CreateInstance<TState>();
        }
        catch
        {
            return default!;
        }
    }
}

namespace Coretech9.Kafkas;

/// <summary>
/// Skip message definition
/// </summary>
/// <typeparam name="TMessage"></typeparam>
public class SkippedMessage<TMessage>
{
    /// <summary>
    /// Message will not consumed before that time (in unix seconds)
    /// </summary>
    public long ConsumeAfter { get; set; }

    /// <summary>
    /// Skipped message
    /// </summary>
    public TMessage Message { get; set; }
}
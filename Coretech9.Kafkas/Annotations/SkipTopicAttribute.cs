namespace Coretech9.Kafkas.Annotations;

/// <summary>
/// Skip topic algorithm definition attribute
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class SkipTopicAttribute : Attribute
{
    /// <summary>
    /// Skip Topic name for skipping messages.
    /// Messages are commited on main topic and produced into that topic
    /// </summary>
    public string Topic { get; set; }

    /// <summary>
    /// Messages in skip topics are not consumed for a specific duration
    /// </summary>
    public TimeSpan SkipDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Skip topic message limit.
    /// If more messages should move into skip topic than that limit, skip operation and main topic consume operations will be stopped.
    /// </summary>
    public int SkipLimit { get; set; } = 10;

    /// <summary>
    /// Retry delay for skipped topic message
    /// </summary>
    public TimeSpan SkipFailRetryDelay { get; set; } = TimeSpan.FromSeconds(5);
}
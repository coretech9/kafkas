using Coretech9.Kafkas.Annotations;

namespace Coretech9.Kafkas;

/// <summary>
/// Consumer options
/// </summary>
public class ConsumerOptions
{
    /// <summary>
    /// Target topic name
    /// </summary>
    public string Topic { get; set; }

    /// <summary>
    /// Target partition number
    /// </summary>
    public int? Partition { get; set; }

    /// <summary>
    /// Consumer group id
    /// </summary>
    public string? ConsumerGroupId { get; set; }

    /// <summary>
    /// Retry count
    /// </summary>
    public int RetryCount { get; set; }

    /// <summary>
    /// Retry wait in milliseconds between failed consume operations
    /// </summary>
    public int RetryWaitMilliseconds { get; set; }

    /// <summary>
    /// Retry wait strategy
    /// </summary>
    public WaitStrategy RetryWaitStrategy { get; set; }

    /// <summary>
    /// If true, consume operation will be committed after the message is produced to error topic
    /// </summary>
    public bool CommitErrorMessages { get; set; }

    /// <summary>
    /// Error topic generator function
    /// </summary>
    public Func<ConsumingMessageMeta, string>? ErrorTopicGenerator { get; set; }
}
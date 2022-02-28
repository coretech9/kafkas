namespace Coretech9.Kafkas.Annotations;

/// <summary>
/// Consume retry count
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class RetryAttribute : Attribute
{
    /// <summary>
    /// Total retry count
    /// </summary>
    public int Count { get; set; }

    /// <summary>
    /// Wait time in milliseconds when a consume operation is failed
    /// </summary>
    public int WaitMilliseconds { get; set; }

    /// <summary>
    /// Wait time strategy for retry operations
    /// </summary>
    public WaitStrategy Strategy { get; set; }

    /// <summary>
    /// Creates new retry attribute
    /// </summary>
    /// <param name="count">Maximum retry count</param>
    /// <param name="waitMilliseconds">Retry wait duration between failed consume operations</param>
    /// <param name="strategy">Retry wait strategy</param>
    public RetryAttribute(int count, int waitMilliseconds, WaitStrategy strategy = WaitStrategy.Fixed)
    {
        Count = count;
        WaitMilliseconds = waitMilliseconds;
        Strategy = strategy;
    }
}
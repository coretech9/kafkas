namespace Coretech9.Kafkas;

/// <summary>
/// The strategy for the consume operation failed messages.
/// </summary>
public enum FailedMessageStrategy
{
    /// <summary>
    /// Ignores the message and moves on.
    /// The message will be LOST!
    /// </summary>
    Ignore,

    /// <summary>
    /// Produces the message to the error topic.
    /// After producing to error topic operation, the message is commited.
    /// </summary>
    ProduceError,

    /// <summary>
    /// Produces the message to the same topic.
    /// After producing to error topic operation, the message is commited.
    /// Same message will be duplicated in same topic.
    /// </summary>
    Reproduce,

    /// <summary>
    /// Retries the consume operation forever.
    /// </summary>
    Retry,

    /// <summary>
    /// Stops consuming operation until application restarts.
    /// </summary>
    Stop
}
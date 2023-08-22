using Confluent.Kafka;

namespace Coretech9.Kafkas;

/// <summary>
/// Kafka log handler event args
/// </summary>
public class ProducerLogEventArgs
{
    /// <summary>
    /// Log message
    /// </summary>
    public LogMessage Message { get; }

    /// <summary>
    /// Creates new log event args
    /// </summary>
    public ProducerLogEventArgs(LogMessage message)
    {
        Message = message;
    }
}
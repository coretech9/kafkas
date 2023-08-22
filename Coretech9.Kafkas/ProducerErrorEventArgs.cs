using Confluent.Kafka;

namespace Coretech9.Kafkas;

/// <summary>
/// Kafka log handler event args
/// </summary>
public class ProducerErrorEventArgs
{
    /// <summary>
    /// Error message
    /// </summary>
    public Error Error { get; }

    /// <summary>
    /// Creates new log event args
    /// </summary>
    public ProducerErrorEventArgs(Error error)
    {
        Error = error;
    }
}
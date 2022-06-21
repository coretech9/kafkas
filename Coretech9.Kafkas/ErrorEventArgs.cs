using Confluent.Kafka;

namespace Coretech9.Kafkas;

/// <summary>
/// Kafka log handler event args
/// </summary>
public class ErrorEventArgs
{
    /// <summary>
    /// Consumer type
    /// </summary>
    public Type ConsumerType { get; }

    /// <summary>
    /// Consuming Message Type
    /// </summary>
    public Type MessageType { get; }

    /// <summary>
    /// Error message
    /// </summary>
    public Error Error { get; }

    /// <summary>
    /// Service Provider
    /// </summary>
    public IServiceProvider ServiceProvider { get; }

    /// <summary>
    /// Creates new log event args
    /// </summary>
    public ErrorEventArgs(Type consumerType, Type messageType, Error error, IServiceProvider serviceProvider)
    {
        ConsumerType = consumerType;
        MessageType = messageType;
        Error = error;
        ServiceProvider = serviceProvider;
    }
}
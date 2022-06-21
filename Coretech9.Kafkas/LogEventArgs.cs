using Confluent.Kafka;

namespace Coretech9.Kafkas;

/// <summary>
/// Kafka log handler event args
/// </summary>
public class LogEventArgs
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
    /// Log message
    /// </summary>
    public LogMessage Message { get; }

    /// <summary>
    /// Service Provider
    /// </summary>
    public IServiceProvider ServiceProvider { get; }

    /// <summary>
    /// Creates new log event args
    /// </summary>
    public LogEventArgs(Type consumerType, Type messageType, LogMessage message, IServiceProvider serviceProvider)
    {
        ConsumerType = consumerType;
        MessageType = messageType;
        Message = message;
        ServiceProvider = serviceProvider;
    }
}
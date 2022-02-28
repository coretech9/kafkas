using Confluent.Kafka;

namespace Coretech9.Kafkas;

/// <summary>
/// Context for each consume operation
/// </summary>
/// <typeparam name="TMessage">Type of the consuming message</typeparam>
public class ConsumeContext<TMessage>
{
    /// <summary>
    /// Consuming message
    /// </summary>
    public TMessage Message { get; }
    
    /// <summary>
    /// Topic and partition information of the message
    /// </summary>
    public TopicPartition Partition { get; }
    
    /// <summary>
    /// Offset information for the message
    /// </summary>
    public TopicPartitionOffset Offset { get; }
    
    /// <summary>
    /// Message total retry count
    /// </summary>
    public int RetryCount { get; set; }

    /// <summary>
    /// Creates new consume context
    /// </summary>
    /// <param name="message">Consuming message</param>
    /// <param name="partition">Topic and partition information of the message</param>
    /// <param name="offset">Offset information for the message</param>
    /// <param name="retryCount">Message total retry count</param>
    public ConsumeContext(TMessage message, TopicPartition partition, TopicPartitionOffset offset, int retryCount)
    {
        Message = message;
        Partition = partition;
        Offset = offset;
        RetryCount = retryCount;
    }
}
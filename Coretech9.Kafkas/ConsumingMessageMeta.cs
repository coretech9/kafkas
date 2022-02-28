using Confluent.Kafka;

namespace Coretech9.Kafkas;

/// <summary>
/// Consuming message information
/// </summary>
public class ConsumingMessageMeta
{
    /// <summary>
    /// Type of consumer message
    /// </summary>
    public Type MessageType { get; }

    /// <summary>
    /// Topic and partition information for the message
    /// </summary>
    public TopicPartition TopicPartition { get; }

    /// <summary>
    /// Partition offset information
    /// </summary>
    public TopicPartitionOffset Offset { get; }

    /// <summary>
    /// Creates new consuming message data
    /// </summary>
    public ConsumingMessageMeta(Type messageType, TopicPartition partition, TopicPartitionOffset offset)
    {
        MessageType = messageType;
        TopicPartition = partition;
        Offset = offset;
    }
}
using Confluent.Kafka;
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
    public string ConsumerGroupId { get; set; }

    /// <summary>
    /// Retry count
    /// </summary>
    public int RetryCount { get; set; } = 0;

    /// <summary>
    /// Retry wait in milliseconds between failed consume operations
    /// </summary>
    public int RetryWaitMilliseconds { get; set; } = 50;

    /// <summary>
    /// Timeout value in milliseconds while attempting to consume next message
    /// </summary>
    public int ConsumeTimeout { get; set; } = 5000;

    /// <summary>
    /// If true, consumer restarts when Unknown Member error is occured. Default value is true.
    /// </summary>
    public bool RestartOnUnknownMemberError { get; set; } = true;

    /// <summary>
    /// Retry wait strategy
    /// </summary>
    public WaitStrategy RetryWaitStrategy { get; set; } = WaitStrategy.Fixed;

    /// <summary>
    /// Waits in milliseconds when a message consume operation is failed.
    /// Default value is 60000. Minimum value is 10.
    /// Lowering that value may cause unnecessary loop when all messages are failed in partition.
    /// </summary>
    public int FailedMessageDelay { get; set; } = 60000;

    /// <summary>
    /// Failed message stragety.
    /// Ignore, just ignores the message. The message data will be lost!
    /// ProduceError, produces the message to speficied error topic and commits the message.
    /// Reproduce, produces the message to end of the same topic and commits the message.
    /// Retry, retries forever.
    /// Stop, stops the consume operations for the consumer.
    /// </summary>
    public FailedMessageStrategy FailedMessageStrategy { get; set; } = FailedMessageStrategy.Retry;

    /// <summary>
    /// Error topic generator function
    /// </summary>
    public Func<ConsumingMessageMeta, Tuple<string, int>> ErrorTopicGenerator { get; set; }

    /// <summary>
    /// Custom log handler implementation for kafka client
    /// </summary>
    public Action<LogEventArgs> LogHandler { get; set; }

    /// <summary>
    /// Custom error handler implementation for kafka client
    /// </summary>
    public Action<ErrorEventArgs> ErrorHandler { get; set; }
}
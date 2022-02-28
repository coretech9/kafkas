namespace Coretech9.Kafkas;

/// <summary>
/// Kafka topic consumer implementation
/// </summary>
/// <typeparam name="TMessage">Consuming message type</typeparam>
public interface ITopicConsumer<TMessage>
{
    /// <summary>
    /// Executes a message consume operation
    /// </summary>
    /// <param name="consumeContext">Consuming message context</param>
    /// <returns></returns>
    Task Consume(ConsumeContext<TMessage> consumeContext);

    /// <summary>
    /// Executed when retry is active and consume operation fails
    /// </summary>
    /// <param name="consumeContext">Consuming message context</param>
    /// <param name="exception">The exception thrown on consume operation</param>
    /// <returns></returns>
    public Task RetryFallback(ConsumeContext<TMessage> consumeContext, Exception exception)
    {
        return Task.CompletedTask;
    }
}
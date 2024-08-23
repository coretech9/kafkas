namespace Coretech9.Kafkas;

/// <summary>
/// ITopicConsumer Interceptor
/// </summary>
public interface IKafkasInterceptor
{
    /// <summary>
    /// Executes a message consume operation
    /// </summary>
    /// <param name="consumeContext">Consuming message context</param>
    /// <returns></returns>
    Task Handle(ConsumeContext consumeContext);
}
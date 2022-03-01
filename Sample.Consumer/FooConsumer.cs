using Coretech9.Kafkas;
using Coretech9.Kafkas.Annotations;

namespace Sample.Consumer;

[Topic("Foo")]
[ErrorTopic("Foo_Error")]
[ConsumerGroupId("Group1")]
[Retry(5, 50, WaitStrategy.Multiplier)]
public class FooConsumer : ITopicConsumer<Foo>
{
    public Task Consume(ConsumeContext<Foo> consumeContext)
    {
        throw new NotImplementedException();
    }
    
    public Task RetryFallback(ConsumeContext<Foo> consumeContext, Exception exception)
    {
        return Task.CompletedTask;
    }
}
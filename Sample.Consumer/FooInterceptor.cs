using Coretech9.Kafkas;

namespace Sample.Consumer;

public class FooInterceptor : IKafkasInterceptor
{
    public Task Handle(ConsumeContext consumeContext)
    {
        throw new NotImplementedException();
    }
}
using Coretech9.Kafkas;
using Microsoft.Extensions.Hosting;
using Sample.Consumer;

IHost host = Host.CreateDefaultBuilder()
    .UseKafkas(builder =>
    {
        builder.AddConsumer<FooConsumer2, Foo>(o => { o.Partition = 5; });
        builder.AddInterceptor<FooInterceptor>();
    })
    .Build();

host.Run();

using Coretech9.Kafkas;
using Microsoft.Extensions.Hosting;
using Sample.Consumer;

IHost host = Host.CreateDefaultBuilder()
    .UseKafkas(builder =>
    {
        builder.AddConsumer<FooConsumer, Foo>(o =>
        {
            o.Partition = 4;
        });
        
        builder.AddConsumer<FooConsumer2, Foo>(o =>
        {
            o.Partition = 5;
        });
    })
    .Build();

host.Run();
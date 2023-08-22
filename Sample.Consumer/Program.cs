using Coretech9.Kafkas;
using Microsoft.Extensions.Hosting;
using Sample.Consumer;

IHost host = Host.CreateDefaultBuilder()
    .UseKafkas(builder =>
    {
        builder.ConfigureDefaultOptions(o =>
        {
            o.LogHandler = e1 => { Console.WriteLine("Log: " + e1.Message.Message); };
            o.ErrorHandler = e2 => { Console.WriteLine("Err: " + e2.Error.Reason); };
        });
        builder.AddConsumer<FooConsumer, Foo>(o =>
        {
            o.Partition = 4;
        });

        builder.AddConsumer<FooConsumer2, Foo>(o => { o.Partition = 5; });
    })
    .Build();

host.Run();

﻿using Coretech9.Kafkas;
using Microsoft.Extensions.Hosting;
using Sample.Consumer;

IHost host = Host.CreateDefaultBuilder()
    .UseKafkas(builder =>
    {
        builder.AddConsumers(typeof(Program));

        builder.AddConsumer<FooConsumer, Foo>(o =>
        {
            o.Topic = "CustomTopic";
        });
    })
    .Build();

host.Run();
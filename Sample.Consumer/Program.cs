using Coretech9.Kafkas;
using Microsoft.Extensions.Hosting;

IHost host = Host.CreateDefaultBuilder()
    .UseKafkas(builder => builder.AddConsumers(typeof(Program)))
    .Build();

host.Run();
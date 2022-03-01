using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Coretech9.Kafkas;

/// <summary>
/// IServiceCollection and IHostBuilder implementations for kafkas
/// </summary>
public static class ConsumerExtensions
{
    /// <summary>
    /// Uses kafkas
    /// </summary>
    /// <param name="services">MSDI services</param>
    /// <param name="cfg">Configuration action</param>
    /// <returns></returns>
    public static IServiceCollection UseKafkas(this IServiceCollection services, Action<KafkasBuilder> cfg)
    {
        KafkasBuilder builder = new KafkasBuilder(services, null);
        services.AddHostedService(p =>
        {
            ProducerConfig config = builder.CreateProducerConfig();
            builder.Producer.Initialize(config, p.GetService<ILogger<KafkasProducer>>());
            return builder.Producer;
        });
        cfg(builder);
        return services;
    }

    /// <summary>
    /// Uses kafkas
    /// </summary>
    /// <param name="host">Host builder</param>
    /// <param name="cfg">Configuration action</param>
    /// <returns></returns>
    public static IHostBuilder UseKafkas(this IHostBuilder host, Action<KafkasBuilder> cfg)
    {
        host.ConfigureServices((context, services) =>
        {
            KafkasBuilder builder = new KafkasBuilder(services, context.Configuration);
            services.AddHostedService(p =>
            {
                builder.Producer.Initialize(builder.CreateProducerConfig(), p.GetService<ILogger<KafkasProducer>>());
                return builder.Producer;
            });
            cfg(builder);
        });

        return host;
    }
}
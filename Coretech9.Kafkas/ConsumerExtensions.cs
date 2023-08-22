using System.Runtime.CompilerServices;
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
    public static IServiceCollection UseKafkas(this IServiceCollection services, Action<KafkasBuilder> cfg = null)
    {
        KafkasBuilder builder = new KafkasBuilder(services, null, null);
        cfg?.Invoke(builder);

        return services;
    }

    /// <summary>
    /// Uses kafkas
    /// </summary>
    /// <param name="host">Host builder</param>
    /// <param name="cfg">Configuration action</param>
    /// <returns></returns>
    public static IHostBuilder UseKafkas(this IHostBuilder host, Action<KafkasBuilder> cfg = null)
    {
        host.ConfigureServices((context, services) =>
        {
            KafkasBuilder builder = new KafkasBuilder(services, context, context.Configuration);
            cfg?.Invoke(builder);
        });

        return host;
    }
}
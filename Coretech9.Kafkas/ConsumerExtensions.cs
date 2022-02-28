using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

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
        KafkasBuilder context = new KafkasBuilder(services, null);
        cfg(context);
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
            KafkasBuilder kafkasBuilder = new KafkasBuilder(services, context.Configuration);
            cfg(kafkasBuilder);
        });

        return host;
    }
}
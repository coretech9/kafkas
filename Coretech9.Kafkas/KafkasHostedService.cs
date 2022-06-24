using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Coretech9.Kafkas;

internal class KafkasHostedService : IHostedService
{
    private readonly List<KafkasRunnerDescriptor> _runners = new List<KafkasRunnerDescriptor>();
    private IServiceProvider _provider;
    private ILogger<KafkasHostedService> _logger;

    internal TimeSpan GracefulWait { get; set; } = TimeSpan.Zero;
    internal Action GracefulAlert { get; set; }

    internal void SetServiceProvider(IServiceProvider provider)
    {
        _provider = provider;
        _logger = _provider.GetService<ILogger<KafkasHostedService>>();
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_provider == null)
        {
            _logger?.LogCritical("Service Provider is null. Kafkas is not initialized correctly!");
            throw new InvalidOperationException("Service Provider is null. Kafkas is not initialized correctly!");
        }

        foreach (KafkasRunnerDescriptor descriptor in _runners)
        {
            _logger?.LogInformation("Initializing kafkas service: {serviceName}", descriptor.Runner.ToString());
            descriptor.InitAction(_provider);
            await descriptor.Runner.StartAsync(cancellationToken);
            _logger?.LogInformation("Kafkas service started: {serviceName}", descriptor.Runner.ToString());
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger?.LogInformation("Stopping Kafkas Services...");

        try
        {
            GracefulAlert?.Invoke();
        }
        catch
        {
        }

        List<Task> tasks = new List<Task>();
        foreach (KafkasRunnerDescriptor descriptor in _runners)
        {
            tasks.Add(descriptor.Runner.StopAsync(cancellationToken));
        }

        if (GracefulWait > TimeSpan.Zero)
        {
            await Task.Delay(Convert.ToInt32(GracefulWait.TotalMilliseconds), cancellationToken);
        }

        Task.WaitAll(tasks.ToArray(), cancellationToken);
        _logger?.LogInformation("Kafkas Services are stopped");

        await Task.Delay(500, cancellationToken);
    }

    internal void AddRunner(KafkasRunner runner, Action<IServiceProvider> initAction)
    {
        _runners.Add(new KafkasRunnerDescriptor(runner, initAction));
    }
}
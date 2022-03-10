namespace Coretech9.Kafkas;

internal class KafkasRunnerDescriptor
{
    internal KafkasRunner Runner { get; init; }
    internal Action<IServiceProvider> InitAction { get; init; }

    public KafkasRunnerDescriptor(KafkasRunner runner, Action<IServiceProvider> initAction)
    {
        Runner = runner;
        InitAction = initAction;
    }
}
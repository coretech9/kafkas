using Confluent.Kafka;
using Coretech9.Kafkas.Annotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Coretech9.Kafkas;

/// <summary>
/// Manages a type of consumer and all operations
/// </summary>
/// <typeparam name="TMessage">Consuming message type</typeparam>
/// <typeparam name="TConsumer">Consumer type</typeparam>
public class KafkasRunner<TConsumer, TMessage> : KafkasRunner
{
    /// <summary>
    /// Initializes kafka runner
    /// </summary>
    /// <param name="provider">MSDI Service provider</param>
    /// <param name="consumerType">Consumer Type</param>
    /// <param name="options">Consumer options</param>
    /// <param name="consumerConfig">Consumer kafka client config</param>
    public override void Initialize(IServiceProvider provider, Type consumerType, ConsumerOptions options, ConsumerConfig consumerConfig)
    {
        Logger = provider.GetService<ILogger<KafkasRunner>>();
        MessageType = typeof(TMessage);
        base.Initialize(provider, consumerType, options, consumerConfig);
    }

    /// <summary>
    /// Executes consume operation
    /// </summary>
    /// <param name="consumeResult">Consuming message</param>
    /// <exception cref="ArgumentNullException">Thrown when message deserialization failed</exception>
    protected override async Task<FailedMessageStrategy?> Execute(ConsumeResult<string, string> consumeResult)
    {
        TMessage model;
        try
        {
            model = System.Text.Json.JsonSerializer.Deserialize<TMessage>(consumeResult.Message.Value);

            if (model == null)
                throw new ArgumentNullException($"Consume model is null for {typeof(TMessage).FullName}");
        }
        catch (Exception e)
        {
            Logger?.LogError(e, "Model Serialization error for {topicName}", consumeResult.Topic);
            FailedMessageStrategy strategy = await ApplyFailStrategy(consumeResult, e);
            return strategy;
        }

        if (ServiceProvider == null)
            throw new ArgumentNullException($"Service provider is null for {typeof(TMessage).FullName}");


        ConsumeContext<TMessage> context = new ConsumeContext<TMessage>(model, consumeResult.Message.Key, consumeResult.Message.Value, consumeResult.TopicPartition, consumeResult.TopicPartitionOffset, 0);

        while (Running)
        {
            ITopicConsumer<TMessage> messageConsumer = null;

            try
            {
                using IServiceScope scope = ServiceProvider.CreateScope();

                foreach (Type type in InterceptorTypes)
                {
                    var interceptor = (IKafkasInterceptor) scope.ServiceProvider.GetService(type);
                    await interceptor.Handle(context);
                }

                messageConsumer = (ITopicConsumer<TMessage>) scope.ServiceProvider.GetService(ConsumerType);

                if (messageConsumer == null)
                    throw new ArgumentNullException($"TopicConsumer is not registered for {typeof(TMessage).FullName}");

                await messageConsumer.Consume(context);
                Consumer?.Commit(consumeResult);

                break;
            }
            catch (Exception e)
            {
                context.RetryCount++;

                if (context.RetryCount >= Options.RetryCount)
                {
                    FailedMessageStrategy strategy = await ApplyFailStrategy(consumeResult, e);
                    Logger?.LogError(e, "Consume operation reached maximum retry count for {topic}", consumeResult.Topic);
                    return strategy;
                }

                Logger?.LogError(e, "Consume operation is {retryCount} times for {topicName}", context.RetryCount, consumeResult.Topic);

                await messageConsumer?.RetryFallback(context, e)!;
                await Task.Delay(CalculateWaitMilliseconds(context.RetryCount));

                if (Options.RestartOnUnknownMemberError && e.Message.Contains("Broker: Unknown member"))
                {
                    Consumer.Close();
                    Consumer.Dispose();
                    await StartAsync(InterceptorTypes, new CancellationToken());
                    Logger?.LogInformation("Resubscribed to {topic} due to unknown member error", Options.Topic);
                }
            }
        }

        return null;
    }

    private async Task<FailedMessageStrategy> ApplyFailStrategy(ConsumeResult<string, string> consumeResult, Exception exception)
    {
        await Task.Delay(Math.Max(10, Options.FailedMessageDelay));

        switch (Options.FailedMessageStrategy)
        {
            case FailedMessageStrategy.ProduceError:
            {
                bool produced = await ProduceErrorMessage(consumeResult);
                if (produced)
                    SafeCommit(consumeResult);
                else
                {
                    await Task.Delay(Math.Max(10, Options.FailedMessageDelay));
                    return FailedMessageStrategy.Retry;
                }

                break;
            }

            case FailedMessageStrategy.Reproduce:
            {
                bool produced = await ReproduceMessage(consumeResult);
                if (produced)
                    SafeCommit(consumeResult);
                else
                {
                    await Task.Delay(Math.Max(10, Options.FailedMessageDelay));
                    return FailedMessageStrategy.Retry;
                }

                break;
            }
        }

        return Options.FailedMessageStrategy;
    }

    private async Task<bool> ReproduceMessage(ConsumeResult<string, string> consumeResult)
    {
        KafkasProducer producer = ServiceProvider.GetService<KafkasProducer>();

        if (producer == null)
            return false;

        try
        {
            await producer.ProduceMessage(Options.Topic, consumeResult.Message)!;
        }
        catch (Exception e)
        {
            Logger?.LogCritical(e, "ReproduceMessage Failed");
            return false;
        }

        return true;
    }

    private async Task<bool> ProduceErrorMessage(ConsumeResult<string, string> consumeResult)
    {
        KafkasProducer producer = ServiceProvider.GetService<KafkasProducer>();

        if (producer == null)
            return false;

        Tuple<string, int> errorTopic = Options.ErrorTopicGenerator?.Invoke(new ConsumingMessageMeta(typeof(TMessage),
                                            consumeResult.TopicPartition,
                                            consumeResult.TopicPartitionOffset))
                                        ?? new Tuple<string, int>(string.Empty, 0);

        if (!string.IsNullOrEmpty(errorTopic.Item1))
        {
            try
            {
                await producer.ProduceMessage(errorTopic.Item1, true, consumeResult.Message)!;
            }
            catch (Exception e)
            {
                Logger?.LogCritical(e, "ProduceErrorMessage Failed");
                return false;
            }
        }

        return true;
    }

    private void SafeCommit(ConsumeResult<string, string> consumeResult)
    {
        try
        {
            Consumer?.Commit(consumeResult);
        }
        catch (Exception e)
        {
            Logger?.LogError(e, "Commit failed message error");
        }
    }

    private int CalculateWaitMilliseconds(int retryCount)
    {
        switch (Options.RetryWaitStrategy)
        {
            case WaitStrategy.Fixed:
                return Options.RetryWaitMilliseconds;

            case WaitStrategy.Multiplier:
                return Options.RetryWaitMilliseconds * retryCount;

            case WaitStrategy.Exponental:
                return Convert.ToInt32(Math.Pow(Options.RetryWaitMilliseconds, retryCount));

            default:
                return Options.RetryWaitMilliseconds;
        }
    }

    /// <summary>
    /// Returns consumer type name
    /// </summary>
    public override string ToString()
    {
        return typeof(TConsumer).FullName ?? "KafkasRunner";
    }
}

/// <summary>
/// Manages a type of consumer and all operations
/// </summary>
public abstract class KafkasRunner
{
    private DateTime _lastTimeoutErrorDate = DateTime.UtcNow.AddMinutes(-60);
    private ConsumerConfig _consumerConfig;
    private bool _busy;

    /// <summary>
    /// Consumer options
    /// </summary>
    protected ConsumerOptions Options { get; private set; } = null!;

    /// <summary>
    /// Kafka consumer client
    /// </summary>
    protected IConsumer<string, string> Consumer { get; private set; }

    /// <summary>
    /// Runner status
    /// </summary>
    protected bool Running { get; private set; }

    /// <summary>
    /// Logger implementation
    /// </summary>
    protected ILogger<KafkasRunner> Logger { get; set; }

    /// <summary>
    /// Service provider for MSDI
    /// </summary>
    protected IServiceProvider ServiceProvider { get; private set; }

    /// <summary>
    /// Consumer Type
    /// </summary>
    protected Type ConsumerType { get; private set; }

    /// <summary>
    /// Consuming Message Type
    /// </summary>
    protected Type MessageType { get; set; }

    /// <summary>
    /// Consumer Interceptor types implemented from IConsumerInterceptor
    /// </summary>
    protected Type[] InterceptorTypes { get; set; }

    /// <summary>
    /// Initializes kafka runner
    /// </summary>
    /// <param name="provider">MSDI Service provider</param>
    /// <param name="consumerType">Consumer Type</param>
    /// <param name="options">Consumer options</param>
    /// <param name="consumerConfig">Consumer kafka client config</param>
    public virtual void Initialize(IServiceProvider provider, Type consumerType, ConsumerOptions options, ConsumerConfig consumerConfig)
    {
        ServiceProvider = provider;
        ConsumerType = consumerType;
        Options = options;
        _consumerConfig = consumerConfig;
    }

    /// <summary>
    /// Starts kafka runner hosted service
    /// </summary>
    /// <param name="interceptors">Interceptor types with IConsumerInterceptor implementation</param>
    /// <param name="cancellationToken">Cancellation token for hosted service</param>
    /// <returns></returns>
    public Task StartAsync(Type[] interceptors, CancellationToken cancellationToken)
    {
        InterceptorTypes = interceptors;
        try
        {
            var builder = new ConsumerBuilder<string, string>(_consumerConfig);

            builder.SetLogHandler((c, m) =>
            {
                Options.LogHandler?.Invoke(new LogEventArgs(Options.Topic, ConsumerType, MessageType, m, ServiceProvider));

                bool shouldShutdown = (!string.IsNullOrEmpty(m.Message) && m.Message.Contains("Connection setup timed out in state CONNECT", StringComparison.InvariantCultureIgnoreCase))
                                      || (!string.IsNullOrEmpty(m.Facility) && m.Facility.Equals("REQTMOUT", StringComparison.InvariantCultureIgnoreCase));

                if (shouldShutdown && Options.ShutdownOnTimeoutErrors)
                {
                    if (DateTime.UtcNow - _lastTimeoutErrorDate < TimeSpan.FromSeconds(300))
                        Environment.Exit(-1);
                    else
                        _lastTimeoutErrorDate = DateTime.UtcNow;
                }
            });

            builder.SetErrorHandler((c, e) =>
            {
                Options?.ErrorHandler(new ErrorEventArgs(ConsumerType, MessageType, e, ServiceProvider));

                if (!string.IsNullOrEmpty(e.Reason) && e.Reason.Contains("Connection refused", StringComparison.InvariantCultureIgnoreCase))
                {
                    if (Options.ShutdownOnTimeoutErrors)
                        Environment.Exit(-1);
                }
            });

            Consumer = builder.Build();
            Consumer.Subscribe(Options.Topic);
        }
        catch (Exception e)
        {
            Logger?.LogCritical(e, "KafkasRunner Intiailization Error");
        }

        _ = Task.Run(RunConsumer, cancellationToken);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Stops kafka runner hosted service
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for hosted service</param>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        Running = false;

        DateTime expiration = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < expiration)
        {
            if (!_busy)
                break;

            await Task.Delay(100, cancellationToken);
        }

        if (Consumer != null)
        {
            Consumer.Close();
            Consumer.Dispose();
        }
    }

    private async Task RunConsumer()
    {
        Running = true;
        _busy = true;

        if (Consumer == null)
        {
            Logger?.LogCritical("Kafkas is not initialized for {type}", GetType().ToString());
            await Task.Delay(1500);
            throw new ArgumentNullException($"Kafkas is not initialized for {GetType()}");
        }

        while (Running)
        {
            try
            {
                ConsumeResult<string, string> result = Consumer.Consume(TimeSpan.FromMilliseconds(Options.ConsumeTimeout));

                if (result == null || result.IsPartitionEOF)
                {
                    await Task.Delay(50);
                    continue;
                }

                execute:
                FailedMessageStrategy? strategy = await Execute(result);

                if (strategy.HasValue)
                {
                    if (strategy.Value == FailedMessageStrategy.Stop)
                        break;

                    if (strategy.Value == FailedMessageStrategy.Shutdown)
                    {
                        Environment.Exit(-1);
                        return;
                    }

                    if (strategy.Value == FailedMessageStrategy.Retry)
                    {
                        await Task.Delay(Options.FailedMessageDelay);
                        goto execute;
                    }
                }
            }
            catch (Exception e)
            {
                Logger?.LogCritical(e, "KafkaRunner Consume operation is failed for {typeName}", GetType().FullName);
                await Task.Delay(5000);
            }
        }

        _busy = false;
    }

    /// <summary>
    /// Executes consume operation
    /// </summary>
    /// <param name="consumeResult">Consuming message</param>
    /// <returns></returns>
    protected abstract Task<FailedMessageStrategy?> Execute(ConsumeResult<string, string> consumeResult);
}
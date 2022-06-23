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
    /// <param name="producer">Producer client</param>
    public override void Initialize(IServiceProvider provider, Type consumerType, ConsumerOptions options, ConsumerConfig consumerConfig, KafkasProducer producer)
    {
        Logger = provider.GetService<ILogger<KafkasRunner>>();
        MessageType = typeof(TMessage);
        base.Initialize(provider, consumerType, options, consumerConfig, producer);
    }

    /// <summary>
    /// Executes consume operation
    /// </summary>
    /// <param name="consumeResult">Consuming message</param>
    /// <exception cref="ArgumentNullException">Thrown when message deserialization failed</exception>
    protected override async Task<FailedMessageStrategy?> Execute(ConsumeResult<Null, string> consumeResult)
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

        ConsumeContext<TMessage> context = new ConsumeContext<TMessage>(model, consumeResult.TopicPartition, consumeResult.TopicPartitionOffset, 0);

        while (Running)
        {
            ITopicConsumer<TMessage> messageConsumer = null;

            try
            {
                using IServiceScope scope = ServiceProvider.CreateScope();
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
            }
        }

        return null;
    }

    private async Task<FailedMessageStrategy> ApplyFailStrategy(ConsumeResult<Null, string> consumeResult, Exception exception)
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

    private async Task<bool> ReproduceMessage(ConsumeResult<Null, string> consumeResult)
    {
        if (Producer == null)
            return false;

        try
        {
            await Producer?.ProduceMessage(Options.Topic, consumeResult)!;
        }
        catch (Exception e)
        {
            Logger?.LogCritical(e, "ReproduceMessage Failed");
            return false;
        }

        return true;
    }

    private async Task<bool> ProduceErrorMessage(ConsumeResult<Null, string> consumeResult)
    {
        if (Producer == null)
            return false;

        Tuple<string, int> errorTopic = Options.ErrorTopicGenerator?.Invoke(new ConsumingMessageMeta(typeof(TMessage),
                                            consumeResult.TopicPartition,
                                            consumeResult.TopicPartitionOffset))
                                        ?? new Tuple<string, int>(string.Empty, 0);

        if (!string.IsNullOrEmpty(errorTopic.Item1))
        {
            try
            {
                await Producer?.ProduceErrorMessage(errorTopic.Item1, consumeResult)!;
            }
            catch (Exception e)
            {
                Logger?.LogCritical(e, "ProduceErrorMessage Failed");
                return false;
            }
        }

        return true;
    }

    private void SafeCommit(ConsumeResult<Null, string> consumeResult)
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
    private ConsumerConfig _consumerConfig;
    private bool _busy;

    /// <summary>
    /// Consumer options
    /// </summary>
    protected ConsumerOptions Options { get; private set; } = null!;

    /// <summary>
    /// Kafka consumer client
    /// </summary>
    protected IConsumer<Null, string> Consumer { get; private set; }

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
    /// Kafkas Producer and Admin Client Manager
    /// </summary>
    public KafkasProducer Producer { get; private set; }

    /// <summary>
    /// Consuming Message Type
    /// </summary>
    protected Type MessageType { get; set; }

    /// <summary>
    /// Initializes kafka runner
    /// </summary>
    /// <param name="provider">MSDI Service provider</param>
    /// <param name="consumerType">Consumer Type</param>
    /// <param name="options">Consumer options</param>
    /// <param name="consumerConfig">Consumer kafka client config</param>
    /// <param name="producer">Kafka producer and admin client manager</param>
    public virtual void Initialize(IServiceProvider provider, Type consumerType, ConsumerOptions options, ConsumerConfig consumerConfig, KafkasProducer producer)
    {
        ServiceProvider = provider;
        ConsumerType = consumerType;
        Options = options;
        _consumerConfig = consumerConfig;
        Producer = producer;
        Producer.Options = options;
    }

    /// <summary>
    /// Starts kafka runner hosted service
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for hosted service</param>
    /// <returns></returns>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            var builder = new ConsumerBuilder<Null, string>(_consumerConfig);

            if (Options.LogHandler != null)
                builder.SetLogHandler((c, m) => Options.LogHandler(new LogEventArgs(ConsumerType, MessageType, m, ServiceProvider)));

            if (Options.ErrorHandler != null)
                builder.SetErrorHandler((c, e) => Options.ErrorHandler(new ErrorEventArgs(ConsumerType, MessageType, e, ServiceProvider)));

            Consumer = builder.Build();

            if (Producer != null)
                await Producer.CheckAndCreateTopic(Options.Topic);

            Consumer.Subscribe(Options.Topic);

            if (Options.FailedMessageStrategy == FailedMessageStrategy.ProduceError)
            {
                TopicPartition partition = new TopicPartition(Options.Topic, Partition.Any);
                TopicPartitionOffset offset = new TopicPartitionOffset(partition, new Offset(0));
                ConsumingMessageMeta meta = new ConsumingMessageMeta(MessageType, partition, offset);

                Tuple<string, int> errorTopic = Options.ErrorTopicGenerator?.Invoke(meta) ?? new Tuple<string, int>(string.Empty, 0);

                if (!string.IsNullOrEmpty(errorTopic.Item1) && Producer != null)
                    await Producer.CheckErrorTopic(errorTopic.Item1, errorTopic.Item2);
            }
        }
        catch (Exception e)
        {
            Logger?.LogCritical(e, "KafkasRunner Intiailization Error");
        }

        _ = Task.Run(RunConsumer, cancellationToken);
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
                ConsumeResult<Null, string> result = Consumer.Consume(TimeSpan.FromSeconds(5));

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

                    if (strategy.Value == FailedMessageStrategy.Stop)
                        Environment.Exit(-1);

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
    protected abstract Task<FailedMessageStrategy?> Execute(ConsumeResult<Null, string> consumeResult);
}
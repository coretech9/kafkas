using Confluent.Kafka;
using Coretech9.Kafkas.Annotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Coretech9.Kafkas;

/// <summary>
/// Manages a type of consumer and all operations
/// </summary>
/// <typeparam name="TMessage">Consuming message type</typeparam>
public class KafkasRunner<TMessage> : KafkasRunner
{
    /// <summary>
    /// Initializes kafka runner
    /// </summary>
    /// <param name="provider">MSDI Service provider</param>
    /// <param name="consumerType">Consumer Type</param>
    /// <param name="options">Consumer options</param>
    /// <param name="consumerConfig">Consumer kafka client config</param>
    /// <param name="producerConfig">Producer kafka client config</param>
    public override void Initialize(IServiceProvider provider, Type consumerType, ConsumerOptions options, ConsumerConfig consumerConfig, ProducerConfig? producerConfig)
    {
        Logger = provider.GetService<ILogger<KafkasRunner>>();
        base.Initialize(provider, consumerType, options, consumerConfig, producerConfig);
    }

    /// <summary>
    /// Executes consume operation
    /// </summary>
    /// <param name="consumeResult">Consuming message</param>
    /// <exception cref="ArgumentNullException">Thrown when message deserialization failed</exception>
    protected override async Task Execute(ConsumeResult<Null, string> consumeResult)
    {
        TMessage? model;
        try
        {
            model = System.Text.Json.JsonSerializer.Deserialize<TMessage>(consumeResult.Message.Value);

            if (model == null)
                throw new ArgumentNullException($"Consume model is null for {typeof(TMessage).FullName}");
        }
        catch (Exception e)
        {
            Logger?.LogError(e, "Model Serialization error for {topicName}", consumeResult.Topic);
            await ProduceErrorMessage(consumeResult);

            if (Options.CommitErrorMessages)
                Consumer?.Commit(consumeResult);

            return;
        }

        if (ServiceProvider == null)
            throw new ArgumentNullException($"Service provider is null for {typeof(TMessage).FullName}");

        ConsumeContext<TMessage> context = new ConsumeContext<TMessage>(model, consumeResult.TopicPartition, consumeResult.TopicPartitionOffset, 0);

        while (Running)
        {
            ITopicConsumer<TMessage>? messageConsumer = null;

            try
            {
                using IServiceScope scope = ServiceProvider.CreateScope();
                messageConsumer = (ITopicConsumer<TMessage>?) scope.ServiceProvider.GetService(ConsumerType);

                if (messageConsumer == null)
                    throw new ArgumentNullException($"TopicConsumer is not registered for {typeof(TMessage).FullName}");

                await messageConsumer.Consume(context);

                if (Options != null && Options.CommitErrorMessages)
                    Consumer?.Commit(consumeResult);

                break;
            }
            catch (ArgumentNullException)
            {
                throw;
            }
            catch (Exception e)
            {
                context.RetryCount++;

                if (context.RetryCount >= Options!.RetryCount)
                {
                    await ProduceErrorMessage(consumeResult);

                    if (Options.CommitErrorMessages)
                        Consumer?.Commit(consumeResult);

                    Logger?.LogError(e, "Consume operation reached maximum retry count for {topic}", consumeResult.Topic);
                    return;
                }

                Logger?.LogError(e, "Consume operation is {retryCount} times for {topicName}", context.RetryCount, consumeResult.Topic);

                await messageConsumer?.RetryFallback(context, e)!;
                await Task.Delay(CalculateWaitMilliseconds(context.RetryCount));
            }
        }
    }

    private async Task ProduceErrorMessage(ConsumeResult<Null, string> consumeResult)
    {
        if (Options == null || Producer == null || !Options.UseErrorTopics)
            return;

        string? errorTopicName = Options.ErrorTopicGenerator?.Invoke(new ConsumingMessageMeta(typeof(TMessage), consumeResult.TopicPartition, consumeResult.TopicPartitionOffset));

        if (errorTopicName != null)
            await Producer?.ProduceAsync(errorTopicName, consumeResult.Message)!;
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
}

/// <summary>
/// Manages a type of consumer and all operations
/// </summary>
public abstract class KafkasRunner : IHostedService
{
    private ProducerConfig? _producerConfig;
    private ConsumerConfig? _consumerConfig;
    private bool _busy;

    /// <summary>
    /// Consumer options
    /// </summary>
    protected ConsumerOptions Options { get; private set; } = null!;

    /// <summary>
    /// Producer client for producing failed messages to error topic
    /// </summary>
    protected IProducer<Null, string>? Producer { get; private set; }

    /// <summary>
    /// Kafka consumer client
    /// </summary>
    protected IConsumer<Null, string>? Consumer { get; private set; }

    /// <summary>
    /// Runner status
    /// </summary>
    protected bool Running { get; private set; }

    /// <summary>
    /// Logger implementation
    /// </summary>
    protected ILogger<KafkasRunner>? Logger { get; set; }

    /// <summary>
    /// Service provider for MSDI
    /// </summary>
    protected IServiceProvider? ServiceProvider { get; private set; }

    /// <summary>
    /// Consumer Type
    /// </summary>
    protected Type ConsumerType { get; private set; }

    /// <summary>
    /// Initializes kafka runner
    /// </summary>
    /// <param name="provider">MSDI Service provider</param>
    /// <param name="consumerType">Consumer Type</param>
    /// <param name="options">Consumer options</param>
    /// <param name="consumerConfig">Consumer kafka client config</param>
    /// <param name="producerConfig">Producer kafka client config</param>
    public virtual void Initialize(IServiceProvider provider, Type consumerType, ConsumerOptions options, ConsumerConfig consumerConfig, ProducerConfig? producerConfig)
    {
        ServiceProvider = provider;
        ConsumerType = consumerType;
        Options = options;
        _consumerConfig = consumerConfig;
        _producerConfig = producerConfig;
    }

    /// <summary>
    /// Starts kafka runner hosted service
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for hosted service</param>
    /// <returns></returns>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_producerConfig != null)
            StartProducerClient();

        StartConsumerClient();

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

        Producer?.Dispose();
    }

    private void StartProducerClient()
    {
        if (Options.UseErrorTopics)
            Producer = new ProducerBuilder<Null, string>(_producerConfig).Build();
    }

    private void StartConsumerClient()
    {
        Consumer = new ConsumerBuilder<Null, string>(_consumerConfig).Build();
        Consumer.Subscribe(Options.Topic);

        Task.Run(RunConsumer);
    }

    private async Task RunConsumer()
    {
        Running = true;
        _busy = true;

        if (Consumer == null)
            throw new ArgumentNullException($"Kafkas is not initialized for {GetType()}");

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

                await Execute(result);
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
    protected abstract Task Execute(ConsumeResult<Null, string> consumeResult);
}
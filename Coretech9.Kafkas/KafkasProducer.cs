using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Coretech9.Kafkas;

/// <summary>
/// Producer and admin client for kafkas.
/// Manages error topics and producing messages to error topics
/// </summary>
public class KafkasProducer : IHostedService
{
    private ProducerConfig? _producerConfig;
    private IProducer<Null, string>? _producer;
    private IAdminClient? _adminClient;
    private readonly List<string> _checkingErrorTopics = new List<string>();
    private ILogger<KafkasProducer>? _logger;
    private Metadata _metadata;

    /// <summary>
    /// Initializes producer and admin kafka clients
    /// </summary>
    /// <param name="producerConfig">Producer client configuration</param>
    /// <param name="logger">Logger</param>
    public void Initialize(ProducerConfig producerConfig, ILogger<KafkasProducer>? logger)
    {
        _logger = logger;
        _producerConfig = producerConfig;

        _producer = new ProducerBuilder<Null, string>(_producerConfig).Build();
        _adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = _producerConfig.BootstrapServers,
            SecurityProtocol = _producerConfig.SecurityProtocol,
            SaslMechanism = _producerConfig.SaslMechanism,
            SaslUsername = _producerConfig.SaslUsername,
            SaslPassword = _producerConfig.SaslPassword,
        }).Build();
    }

    /// <summary>
    /// Produce error message to error topic
    /// </summary>
    /// <param name="errorTopic">Error topic name</param>
    /// <param name="consumeResult">Consuming message</param>
    /// <returns></returns>
    public async Task<bool> ProduceErrorMessage(string errorTopic, ConsumeResult<Null, string> consumeResult)
    {
        try
        {
            await _producer?.ProduceAsync(errorTopic, consumeResult.Message)!;
        }
        catch (Exception e)
        {
            _logger?.LogCritical(e, "ProduceErrorMessage Failed");
            return false;
        }

        return true;
    }

    /// <summary>
    /// Checks error topic name.
    /// If not exists, creates the topic with default partition count.
    /// </summary>
    public async Task CheckErrorTopic(string errorTopicName)
    {
        if (_adminClient == null)
        {
            lock (_checkingErrorTopics)
                _checkingErrorTopics.Add(errorTopicName);

            return;
        }

        if (_metadata == null)
            _metadata = _adminClient.GetMetadata(TimeSpan.FromSeconds(30));

        List<TopicSpecification> list = new List<TopicSpecification>();

        if (!string.IsNullOrEmpty(errorTopicName))
        {
            TopicMetadata? topicMetadata = _metadata.Topics.FirstOrDefault(x => x.Topic == errorTopicName);

            if (topicMetadata == null)
                list.Add(new TopicSpecification {Name = errorTopicName});
        }

        lock (_checkingErrorTopics)
        {
            foreach (string topic in _checkingErrorTopics)
            {
                TopicMetadata? m = _metadata.Topics.FirstOrDefault(x => x.Topic == topic);
                if (m == null)
                    list.Add(new TopicSpecification {Name = topic});
            }

            _checkingErrorTopics.Clear();
        }

        if (list.Count > 0)
            await _adminClient.CreateTopicsAsync(list);
    }

    internal async Task CheckAndCreateTopic(string topicName)
    {
        try
        {
            if (_adminClient == null)
                return;

            if (_metadata == null)
                _metadata = _adminClient.GetMetadata(TimeSpan.FromSeconds(30));

            TopicMetadata? topicMetadata = _metadata.Topics.FirstOrDefault(x => x.Topic == topicName);

            if (topicMetadata == null)
            {
                List<TopicSpecification> list = new List<TopicSpecification> {new TopicSpecification {Name = topicName}};
                await _adminClient.CreateTopicsAsync(list);
            }
        }
        catch (Exception e)
        {
            _logger?.LogError(e, "CheckAndCreateTopic error for {topic}", topicName);
        }
    }

    /// <summary>
    /// Starts producer and admin kafka client for kaskas
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _producer = new ProducerBuilder<Null, string>(_producerConfig).Build();
        await CheckErrorTopic(string.Empty);
    }

    /// <summary>
    /// Stops producer and admin kafka client for kaskas
    /// </summary>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        _producer?.Dispose();
        _adminClient?.Dispose();
        return Task.CompletedTask;
    }
}
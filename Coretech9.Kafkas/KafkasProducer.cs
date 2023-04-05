using System.Reflection;
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
    internal ConsumerOptions Options { get; set; }

    private ProducerConfig _producerConfig;
    private AdminClientConfig _adminConfig;
    private IProducer<string, string> _producer;
    private IAdminClient _adminClient;
    private readonly List<Tuple<string, int>> _checkingErrorTopics = new List<Tuple<string, int>>();
    private ILogger<KafkasProducer> _logger;
    private Metadata _metadata;
    private IServiceProvider _serviceProvider;

    /// <summary>
    /// Initializes producer and admin kafka clients
    /// </summary>
    /// <param name="serviceProvider"></param>
    /// <param name="producerConfig">Producer client configuration</param>
    /// <param name="logger">Logger</param>
    public void Initialize(IServiceProvider serviceProvider, ProducerConfig producerConfig, ILogger<KafkasProducer> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _producerConfig = producerConfig;
        _adminConfig = new AdminClientConfig();
        ApplyProducerConfigToAdmin(_producerConfig, _adminConfig);
        _adminConfig.ClientId = _producerConfig.ClientId + "-admin";
    }

    private void ApplyProducerConfigToAdmin(ProducerConfig producerConfig, AdminClientConfig adminConfig)
    {
        try
        {
            Type adminType = typeof(AdminClientConfig);
            PropertyInfo[] properties = typeof(ProducerConfig).GetProperties();
            foreach (PropertyInfo property in properties)
            {
                PropertyInfo adminProp = adminType.GetProperty(property.Name);
                if (adminProp == null)
                    continue;

                object value = property.GetValue(producerConfig);
                if (value != null)
                    adminProp.SetValue(adminConfig, value);
            }
        }
        catch
        {
        }
    }

    /// <summary>
    /// Produce error message to error topic
    /// </summary>
    /// <param name="errorTopic">Error topic name</param>
    /// <param name="consumeResult">Consuming message</param>
    /// <returns></returns>
    public async Task<bool> ProduceErrorMessage(string errorTopic, ConsumeResult<string, string> consumeResult)
    {
        try
        {
            await _producer?.ProduceAsync(errorTopic, consumeResult.Message)!;
        }
        catch (Exception e)
        {
            _logger?.LogCritical(e, "ProduceErrorMessage Failed: {topic}", errorTopic);
            return false;
        }

        return true;
    }

    /// <summary>
    /// Produce message to error topic
    /// </summary>
    /// <param name="topic">Topic name</param>
    /// <param name="consumeResult">Consuming message</param>
    /// <returns></returns>
    public async Task<bool> ProduceMessage(string topic, ConsumeResult<string, string> consumeResult)
    {
        try
        {
            await _producer?.ProduceAsync(topic, consumeResult.Message)!;
        }
        catch (Exception e)
        {
            _logger?.LogCritical(e, "ProduceMessage Failed: {topic}", topic);
            return false;
        }

        return true;
    }

    /// <summary>
    /// Checks error topic name.
    /// If not exists, creates the topic with default partition count.
    /// </summary>
    public async Task CheckErrorTopic(string errorTopicName, int partitionCount)
    {
        if (_adminClient == null)
        {
            if (!string.IsNullOrEmpty(errorTopicName))
                lock (_checkingErrorTopics)
                    _checkingErrorTopics.Add(new Tuple<string, int>(errorTopicName, partitionCount));

            return;
        }

        if (_metadata == null)
            _metadata = _adminClient.GetMetadata(TimeSpan.FromSeconds(30));

        List<TopicSpecification> list = new List<TopicSpecification>();

        if (!string.IsNullOrEmpty(errorTopicName))
        {
            TopicMetadata topicMetadata = _metadata.Topics.FirstOrDefault(x => x.Topic == errorTopicName);

            if (topicMetadata == null)
                list.Add(new TopicSpecification {Name = errorTopicName, NumPartitions = partitionCount});
        }

        lock (_checkingErrorTopics)
        {
            foreach (Tuple<string, int> topic in _checkingErrorTopics)
            {
                if (string.IsNullOrEmpty(topic.Item1))
                    continue;

                TopicMetadata m = _metadata.Topics.FirstOrDefault(x => x.Topic == topic.Item1);
                if (m == null)
                    list.Add(new TopicSpecification {Name = topic.Item1, NumPartitions = topic.Item2});
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

            TopicMetadata topicMetadata = _metadata.Topics.FirstOrDefault(x => x.Topic == topicName);

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
        var builder = new ProducerBuilder<string, string>(_producerConfig);
        var adminBuilder = new AdminClientBuilder(_adminConfig);

        if (Options?.LogHandler != null)
        {
            builder.SetLogHandler((c, m) => Options.LogHandler(new LogEventArgs(Options.Topic, null, null, m, _serviceProvider)));
            adminBuilder.SetLogHandler((c, m) => Options.LogHandler(new LogEventArgs(Options.Topic, null, null, m, _serviceProvider)));
        }

        if (Options?.ErrorHandler != null)
        {
            builder.SetErrorHandler((c, e) => Options.ErrorHandler(new ErrorEventArgs(null, null, e, _serviceProvider)));
            adminBuilder.SetErrorHandler((c, e) => Options.ErrorHandler(new ErrorEventArgs(null, null, e, _serviceProvider)));
        }

        _producer = builder.Build();
        _adminClient = adminBuilder.Build();

        await CheckErrorTopic(string.Empty, 1);
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
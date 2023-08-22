using System.Reflection;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Coretech9.Kafkas;

/// <summary>
/// Producer client for kafka
/// </summary>
public class KafkasProducer : IHostedService
{
    private ILogger<KafkasProducer> _logger;

    private ProducerConfig _config;
    private IProducer<string, string> _producer;
    private IAdminClient _admin;
    private Metadata _metadata;
    private ProducerOptions _options;
    private IServiceProvider _provider;
    private string[] _createdTopics = Array.Empty<string>();
    private bool _running;

    internal void Initialize(IServiceProvider provider, ProducerOptions options, ProducerConfig config)
    {
        _provider = provider;
        _config = config;
        _options = options;
        _logger = provider.GetService<ILogger<KafkasProducer>>();
    }

    /// <summary>
    /// Starts producer hosted service
    /// </summary>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        var builder = new ProducerBuilder<string, string>(_config);

        AdminClientConfig adminConfig = new AdminClientConfig();
        ApplyProducerConfigToAdmin(_config, adminConfig);
        adminConfig.ClientId += "-admin";

        var adminBuilder = new AdminClientBuilder(adminConfig);

        if (_options?.LogHandler != null)
        {
            builder.SetLogHandler((c, m) => _options.LogHandler(new ProducerLogEventArgs(m)));
            adminBuilder.SetLogHandler((c, m) => _options.LogHandler(new ProducerLogEventArgs(m)));
        }

        if (_options?.ErrorHandler != null)
        {
            builder.SetErrorHandler((c, m) => _options.ErrorHandler(new ProducerErrorEventArgs(m)));
            adminBuilder.SetErrorHandler((c, m) => _options.ErrorHandler(new ProducerErrorEventArgs(m)));
        }

        _producer = builder.Build();
        _admin = adminBuilder.Build();
        _running = true;

        if (_options.HealthCheck > TimeSpan.Zero)
            _ = RunHealthCheck();

        return Task.CompletedTask;
    }

    private async Task RunHealthCheck()
    {
        int ms = Convert.ToInt32(_options.HealthCheck.TotalMilliseconds);
        while (_running)
        {
            await Task.Delay(ms);
            
            if (_running)
                await ProduceMessage(_options.HealthCheckTopicName, new Message<string, string> {Value = "."});
        }
    }

    /// <summary>
    /// Stops kafka producer hosted service
    /// </summary>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        _running = false;
        _producer?.Dispose();
        _admin?.Dispose();
        return Task.CompletedTask;
    }

    private async Task CheckAndCreateTopic(string topicName)
    {
        try
        {
            if (_createdTopics.Contains(topicName))
                return;

            if (_admin == null)
            {
                var adminBuilder = new AdminClientBuilder(_config);
                _admin = adminBuilder.Build();
            }

            if (_metadata == null)
                _metadata = _admin.GetMetadata(TimeSpan.FromSeconds(30));

            TopicMetadata topicMetadata = _metadata.Topics.FirstOrDefault(x => x.Topic == topicName);

            if (topicMetadata == null)
            {
                List<TopicSpecification> list = new List<TopicSpecification> {new TopicSpecification {Name = topicName}};
                await _admin.CreateTopicsAsync(list);
            }

            List<string> errorTopics = _createdTopics.ToList();
            errorTopics.Add(topicName);
            _createdTopics = errorTopics.ToArray();
        }
        catch (Exception e)
        {
            _logger?.LogError(e, "CheckAndCreateTopic error for {topic}", topicName);
        }
    }

    #region Produce

    /// <summary>
    /// Produce message to error topic
    /// </summary>
    /// <param name="topic">Topic name</param>
    /// <param name="message">Message</param>
    /// <returns></returns>
    public Task<bool> ProduceMessage(string topic, Message<string, string> message)
    {
        return ProduceMessage(topic, false, message);
    }

    /// <summary>
    /// Produce message to error topic
    /// </summary>
    /// <param name="topic">Topic name</param>
    /// <param name="createTopicIfNotExists">If true, checks topic existence and creates it</param>
    /// <param name="message">Message</param>
    /// <returns></returns>
    public async Task<bool> ProduceMessage(string topic, bool createTopicIfNotExists, Message<string, string> message)
    {
        try
        {
            if (createTopicIfNotExists)
                await CheckAndCreateTopic(topic);

            await _producer?.ProduceAsync(topic, message)!;
        }
        catch (Exception e)
        {
            _logger?.LogCritical(e, "ProduceMessage Failed: {topic}", topic);
            return false;
        }

        return true;
    }

    #endregion


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
}
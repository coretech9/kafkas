using System.Reflection;
using Confluent.Kafka;
using Coretech9.Kafkas.Annotations;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Coretech9.Kafkas;

/// <summary>
/// Builder for kafkas
/// </summary>
public class KafkasBuilder
{
    private Action<ConsumerConfig> _consumerConfigAction;
    private Action<ProducerConfig> _producerConfigAction;
    private Action<ConsumerOptions> _consumerOptionsAction;
    private readonly IServiceCollection _services;
    private readonly KafkasHostedService _hostedService;
    private string _rootSection = "Kafkas";

    /// <summary>
    /// IHost Configuration
    /// </summary>
    public IConfiguration Configuration { get; }

    /// <summary>
    /// Host Builder Context
    /// </summary>
    public HostBuilderContext Context { get; }

    /// <summary>
    /// Creates new kafkas builder
    /// </summary>
    /// <param name="services">MSDI services</param>
    /// <param name="context">Microsoft Hosting Extension Context</param>
    /// <param name="configuration">Microsoft.Extensions.Hosting configuration</param>
    public KafkasBuilder(IServiceCollection services, HostBuilderContext context, IConfiguration configuration)
    {
        _services = services;
        Context = context;
        Configuration = configuration;
        _hostedService = new KafkasHostedService();
        services.AddHostedService(p =>
        {
            _hostedService.SetServiceProvider(p);
            return _hostedService;
        });
    }

    /// <summary>
    /// Sets default consumer options
    /// </summary>
    /// <param name="options">Action handler for consumer options</param>
    /// <returns></returns>
    public KafkasBuilder ConfigureDefaultOptions(Action<ConsumerOptions> options)
    {
        _consumerOptionsAction = options;
        return this;
    }

    /// <summary>
    /// Sets default configurations for kafka consumer client
    /// </summary>
    /// <param name="config">Action handler for options</param>
    /// <returns></returns>
    public KafkasBuilder ConfigureDefaultConsumer(Action<ConsumerConfig> config)
    {
        _consumerConfigAction = config;
        return this;
    }

    /// <summary>
    /// Sets default configurations for kafka producer client
    /// </summary>
    /// <param name="config">Action handler for options</param>
    /// <returns></returns>
    public KafkasBuilder ConfigureDefaultProducer(Action<ProducerConfig> config)
    {
        _producerConfigAction = config;
        return this;
    }

    /// <summary>
    /// Changes default root section key for appsettings configuration.
    /// Default value is "Kafkas"
    /// </summary>
    /// <param name="root">New root section value</param>
    /// <returns></returns>
    public KafkasBuilder SetRootConfigurationSection(string root)
    {
        _rootSection = root;
        return this;
    }

    /// <summary>
    /// Sets graceful shutdown service.
    /// After SIGTERM is called, alert action is called the application waits at least duration time.
    /// </summary>
    public KafkasBuilder SetGracefulShutdown(TimeSpan duration, Action alertAction)
    {
        _hostedService.GracefulWait = duration;
        _hostedService.GracefulAlert = alertAction;
        return this;
    }

    /// <summary>
    /// Uses producer client
    /// </summary>
    /// <returns></returns>
    public KafkasBuilder UseProducerClient(Action<ProducerOptions> opt)
    {
        _services.AddHostedService(p =>
        {
            ProducerOptions options = new ProducerOptions();
            opt(options);

            KafkasProducer producer = new KafkasProducer();
            ProducerConfig config = CreateProducerConfig();
            producer.Initialize(p, options, config);
            return producer;
        });

        return this;
    }

    /// <summary>
    /// Adds a consumer
    /// </summary>
    /// <typeparam name="TConsumer">Consumer type</typeparam>
    /// <typeparam name="TMessage">Consuming message type</typeparam>
    /// <returns></returns>
    public KafkasBuilder AddConsumer<TConsumer, TMessage>() where TConsumer : class, ITopicConsumer<TMessage>
    {
        KafkasRunner<TConsumer, TMessage> runner = new KafkasRunner<TConsumer, TMessage>();
        _services.AddScoped<TConsumer>();
        _hostedService.AddRunner(runner, p => InitializeKafkaRunner(p, runner, typeof(TConsumer)));
        return this;
    }

    /// <summary>
    /// Adds a consumer
    /// </summary>
    /// <typeparam name="TConsumer">Consumer type</typeparam>
    /// <typeparam name="TMessage">Consuming message type</typeparam>
    /// <returns></returns>
    public KafkasBuilder AddConsumer<TConsumer, TMessage>(Action<ConsumerOptions> options) where TConsumer : class, ITopicConsumer<TMessage>
    {
        KafkasRunner<TConsumer, TMessage> runner = new KafkasRunner<TConsumer, TMessage>();
        _services.AddScoped<TConsumer>();
        _hostedService.AddRunner(runner, p => InitializeKafkaRunner(p, runner, typeof(TConsumer), options));
        return this;
    }

    /// <summary>
    /// Adds all consumers in specified assemblies
    /// </summary>
    /// <param name="assemblyTypes">Types in assemblies</param>
    /// <returns></returns>
    /// <exception cref="ArgumentNullException">Thrown when consumer or kafka runner implementation registrations are missing</exception>
    public KafkasBuilder AddConsumers(params Type[] assemblyTypes)
    {
        Type openGenericType = typeof(ITopicConsumer<>);

        foreach (Type assemblyType in assemblyTypes)
        {
            Type[] types = assemblyType.Assembly.GetTypes();
            foreach (Type type in types)
            {
                if (type.IsInterface || type.IsAbstract)
                    continue;

                Type[] interfaceTypes = type.GetInterfaces();
                foreach (Type interfaceType in interfaceTypes)
                {
                    Type generic = interfaceType.GetGenericTypeDefinition();
                    if (openGenericType.IsAssignableFrom(generic))
                    {
                        //topic consumer found
                        Type modelType = interfaceType.GetGenericArguments().FirstOrDefault();
                        if (modelType == null)
                            throw new ArgumentNullException($"Topic consumer model type is null for {type}");

                        Type runnerType = typeof(KafkasRunner<,>).MakeGenericType(type, modelType);
                        KafkasRunner runner = Activator.CreateInstance(runnerType) as KafkasRunner;

                        if (runner == null)
                            throw new ArgumentNullException($"No valid model type for kafka runner {type}");

                        _services.AddScoped(type, type);
                        _hostedService.AddRunner(runner, p => InitializeKafkaRunner(p, runner, type));
                    }
                }
            }
        }

        return this;
    }

    private void InitializeKafkaRunner(IServiceProvider provider, KafkasRunner runner, Type consumerType, Action<ConsumerOptions> func = null)
    {
        ConsumerOptions options = CreateConsumerOptions(consumerType);

        if (func != null)
            func(options);

        ConsumerConfig consumerConfig = CreateConsumerConfig(options);
        runner.Initialize(provider, consumerType, options, consumerConfig);
    }

    private ConsumerOptions CreateConsumerOptions(Type consumerType)
    {
        ConsumerOptions options = new ConsumerOptions();
        options.ErrorTopicGenerator = m => new Tuple<string, int>($"{m.TopicPartition.Topic}_Error", 1);

        if (Configuration != null)
        {
            ReadFromConfiguration(options, _rootSection);
        }

        _consumerOptionsAction?.Invoke(options);

        TopicAttribute topicAttribute = consumerType.GetCustomAttribute<TopicAttribute>();
        ConsumerGroupIdAttribute groupIdAttribute = consumerType.GetCustomAttribute<ConsumerGroupIdAttribute>();
        ErrorTopicAttribute errorTopicAttribute = consumerType.GetCustomAttribute<ErrorTopicAttribute>();
        RetryAttribute retryAttribute = consumerType.GetCustomAttribute<RetryAttribute>();

        if (topicAttribute != null)
        {
            options.Topic = topicAttribute.Topic;

            if (topicAttribute.Partition.HasValue)
                options.Partition = topicAttribute.Partition;
        }

        if (groupIdAttribute != null)
            options.ConsumerGroupId = groupIdAttribute.ConsumerGroupId;

        if (errorTopicAttribute != null)
            options.ErrorTopicGenerator = _ => new Tuple<string, int>(errorTopicAttribute.Topic, errorTopicAttribute.PartitionCount);

        if (retryAttribute != null)
        {
            options.RetryCount = retryAttribute.Count;
            options.RetryWaitMilliseconds = retryAttribute.WaitMilliseconds;
            options.RetryWaitStrategy = retryAttribute.Strategy;
        }

        return options;
    }

    private ConsumerConfig CreateConsumerConfig(ConsumerOptions options)
    {
        ConsumerConfig config = new ConsumerConfig();
        config.GroupId = options.ConsumerGroupId;
        config.EnableAutoCommit = false;

        if (Configuration != null)
        {
            ReadFromConfiguration(config, _rootSection);
            ReadFromConfiguration(config, $"{_rootSection}:Consumer");
        }

        _consumerConfigAction?.Invoke(config);
        return config;
    }

    private bool ReadFromConfiguration<TConfig>(TConfig config, string path) where TConfig : class
    {
        IConfigurationSection section = Configuration.GetSection(path);

        if (section == null || !section.Exists())
            return false;

        Type configType = config.GetType();
        PropertyInfo[] properties = configType.GetProperties();
        foreach (PropertyInfo property in properties)
        {
            IConfigurationSection child = section.GetSection(property.Name);

            if (!child.Exists())
                continue;

            object value = section.GetValue(property.PropertyType, property.Name);
            if (value != null)
                property.SetValue(config, value);
        }

        return true;
    }

    internal ProducerConfig CreateProducerConfig()
    {
        ProducerConfig config = new ProducerConfig();
        
        if (Configuration != null)
        {
            ReadFromConfiguration(config, _rootSection);

            bool hasProducerConfig = ReadFromConfiguration(config, $"{_rootSection}:Producer");

            if (!hasProducerConfig)
                ReadFromConfiguration(config, $"{_rootSection}:Consumer");
        }

        _producerConfigAction?.Invoke(config);
        return config;
    }
}
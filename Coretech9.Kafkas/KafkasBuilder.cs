﻿using System.Reflection;
using Confluent.Kafka;
using Coretech9.Kafkas.Annotations;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Coretech9.Kafkas;

/// <summary>
/// Builder for kafkas
/// </summary>
public class KafkasBuilder
{
    private Action<ConsumerConfig>? _consumerConfigAction;
    private Action<ProducerConfig>? _producerConfigAction;
    private Action<ConsumerOptions>? _consumerOptionsAction;
    private readonly IServiceCollection _services;
    public IConfiguration? Configuration { get; }
    private string _rootSection = "Kafkas";
    internal KafkasProducer Producer { get; }

    /// <summary>
    /// Creates new kafkas builder
    /// </summary>
    /// <param name="services">MSDI services</param>
    /// <param name="configuration">Microsoft.Extensions.Hosting configuration</param>
    public KafkasBuilder(IServiceCollection services, IConfiguration? configuration)
    {
        _services = services;
        Configuration = configuration;
        Producer = new KafkasProducer();
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
    /// Adds a consumer
    /// </summary>
    /// <typeparam name="TConsumer">Consumer type</typeparam>
    /// <typeparam name="TMessage">Consuming message type</typeparam>
    /// <returns></returns>
    public KafkasBuilder AddConsumer<TConsumer, TMessage>() where TConsumer : class, ITopicConsumer<TMessage>
    {
        KafkasRunner<TMessage> runner = new KafkasRunner<TMessage>();
        _services.AddScoped<TConsumer>();
        _services.AddHostedService(p =>
        {
            InitializeKafkaRunner(p, runner, typeof(TConsumer));
            return runner;
        });

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
        KafkasRunner<TMessage> runner = new KafkasRunner<TMessage>();
        _services.AddScoped<TConsumer>();
        _services.AddHostedService(p =>
        {
            InitializeKafkaRunner(p, runner, typeof(TConsumer), options);
            return runner;
        });

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
                        Type? modelType = interfaceType.GetGenericArguments().FirstOrDefault();
                        if (modelType == null)
                            throw new ArgumentNullException($"Topic consumer model type is null for {type}");

                        Type runnerType = typeof(KafkasRunner<>).MakeGenericType(modelType);
                        KafkasRunner? runner = Activator.CreateInstance(runnerType) as KafkasRunner;

                        if (runner == null)
                            throw new ArgumentNullException($"No valid model type for kafka runner {type}");

                        _services.AddScoped(type, type);
                        _services.AddHostedService(p =>
                        {
                            InitializeKafkaRunner(p, runner, type);
                            return runner;
                        });
                    }
                }
            }
        }

        return this;
    }

    private void InitializeKafkaRunner(IServiceProvider provider, KafkasRunner runner, Type consumerType, Action<ConsumerOptions>? func = null)
    {
        ConsumerOptions options = CreateConsumerOptions(consumerType);

        if (func != null)
            func(options);
        
        ConsumerConfig consumerConfig = CreateConsumerConfig(options);
        runner.Initialize(provider, consumerType, options, consumerConfig, Producer);
    }

    private ConsumerOptions CreateConsumerOptions(Type consumerType)
    {
        ConsumerOptions options = new ConsumerOptions();
        options.ErrorTopicGenerator = m => $"{m.TopicPartition.Topic}_Error";

        if (Configuration != null)
        {
            IConfigurationSection section = Configuration.GetSection($"{_rootSection}:Options");
            options.ConsumerGroupId = section.GetValue<string>("ConsumerGroupId");
            options.RetryCount = section.GetValue<int>("RetryCount");
            options.RetryWaitMilliseconds = section.GetValue<int>("RetryWaitMilliseconds");
            options.CommitErrorMessages = section.GetValue<bool>("CommitErrorMessages");
            options.RetryWaitStrategy = section.GetValue<WaitStrategy>("RetryDelayStrategy");
        }

        _consumerOptionsAction?.Invoke(options);

        TopicAttribute? topicAttribute = consumerType.GetCustomAttribute<TopicAttribute>();
        ConsumerGroupIdAttribute? groupIdAttribute = consumerType.GetCustomAttribute<ConsumerGroupIdAttribute>();
        ErrorTopicAttribute? errorTopicAttribute = consumerType.GetCustomAttribute<ErrorTopicAttribute>();
        RetryAttribute? retryAttribute = consumerType.GetCustomAttribute<RetryAttribute>();

        if (topicAttribute != null)
        {
            options.Topic = topicAttribute.Topic;

            if (topicAttribute.Partition.HasValue)
                options.Partition = topicAttribute.Partition;
        }

        if (groupIdAttribute != null)
            options.ConsumerGroupId = groupIdAttribute.ConsumerGroupId;

        if (errorTopicAttribute != null)
            options.ErrorTopicGenerator = _ => errorTopicAttribute.Topic;

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

        if (Configuration != null)
            ReadFromConfiguration(config, $"{_rootSection}:Consumer");

        _consumerConfigAction?.Invoke(config);
        return config;
    }

    private void ReadFromConfiguration<TConfig>(TConfig config, string path) where TConfig : class
    {
        if (Configuration == null)
            return;

        IConfigurationSection section = Configuration.GetSection(path);

        if (section != null)
        {
            Type configType = config.GetType();
            PropertyInfo[] properties = configType.GetProperties();
            foreach (PropertyInfo property in properties)
            {
                object value = section.GetValue(property.PropertyType, property.Name);
                if (value != null)
                {
                    property.SetValue(config, value);
                }
            }
        }
    }

    internal ProducerConfig CreateProducerConfig()
    {
        ProducerConfig config = new ProducerConfig();

        if (Configuration != null)
            ReadFromConfiguration(config, $"{_rootSection}:Producer");

        _producerConfigAction?.Invoke(config);
        return config;
    }
}
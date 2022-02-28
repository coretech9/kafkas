# Kafkas

C# Consumer Implementation for Apache Kafka.


Sample Usage:


**appsettings.json**

```json
  "Kafkas": {
    "Options": {
      "ConsumerGroupId": "SampleGroupId",
      "RetryCount": 5,
      "RetryWaitMilliseconds": 100,
      "RetryDelayStrategy": "Fixed",
      "CommitErrorMessages": true
    },
    "Consumer": {
      "SaslUsername": "test",
      "SaslPassword": "test",
      "SaslMechanism": "Plain",
      ... These configs ConsumerConfig class properties of Confluent.Kafka package
    },
    "Producer": {
      "SaslUsername": "test",
      "SaslPassword": "test",
      "Acks": "All",
      ... these configs ProducerConfig class properties of Confluent.Kafka package
    }
  }
```

**IHostBuilder Implementation**

```csharp
IHost host = Host.CreateDefaultBuilder()
    .UseKafkas(builder => builder.AddConsumers(typeof(Program)))
    .Build();
```

**Consumer**

```csharp
[Topic("Foo")]
[ErrorTopic("Foo_Error")]
[ConsumerGroupId("Group1")] //optional. if you want to override default options.
[Retry(5, 50, WaitStrategy.Multiplier)] //optional. if you want to override default options.
public class FooConsumer : ITopicConsumer<Foo>
{
    public async Task Consume(ConsumeContext<Foo> consumeContext)
    {
        // do something
    }
    
    //optional interface method implementation. if you want to do something after a failed try.
    public async Task RetryFallback(ConsumeContext<Foo> consumeContext, Exception exception)
    {
        // do something
    }
}
```

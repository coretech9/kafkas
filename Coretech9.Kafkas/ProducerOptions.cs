namespace Coretech9.Kafkas;

/// <summary>
/// Kafkas producer options
/// </summary>
public class ProducerOptions
{
    /// <summary>
    /// If non zero, producer clients sends a message to the server periodically to keep alive the connection.
    /// Default is 30 seconds.
    /// </summary>
    public TimeSpan HealthCheck { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Custom log handler implementation for kafka client
    /// </summary>
    public Action<ProducerLogEventArgs> LogHandler { get; set; }

    /// <summary>
    /// Custom error handler implementation for kafka client
    /// </summary>
    public Action<ProducerErrorEventArgs> ErrorHandler { get; set; }

    /// <summary>
    /// Non exists topic name for health check. Default value is KafkasHealthCheck.
    /// </summary>
    public string HealthCheckTopicName { get; set; } = "KafkasHealthCheck";
}
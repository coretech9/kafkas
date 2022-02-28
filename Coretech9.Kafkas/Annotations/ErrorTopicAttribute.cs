namespace Coretech9.Kafkas.Annotations;

/// <summary>
/// After all retries failed, message is produced to this topic
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class ErrorTopicAttribute : Attribute
{
    /// <summary>
    /// Topic Name for failed messages
    /// </summary>
    public string Topic { get; set; }

    /// <summary>
    /// Creates new error topic attribute
    /// </summary>
    /// <param name="topic">Target topic name for failed messages</param>
    public ErrorTopicAttribute(string topic)
    {
        Topic = topic;
    }
}
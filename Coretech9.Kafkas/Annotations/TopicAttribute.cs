namespace Coretech9.Kafkas.Annotations;

/// <summary>
/// Target topic for consuming messags
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class TopicAttribute : Attribute
{
    /// <summary>
    /// Topic name
    /// </summary>
    public string Topic { get; set; }

    /// <summary>
    /// Partition number. Leave null if you want server manages it.
    /// </summary>
    public int? Partition { get; set; }

    /// <summary>
    /// Creates new topic attribute
    /// </summary>
    /// <param name="topic">Topic name</param>
    public TopicAttribute(string topic)
    {
        Topic = topic;
    }

    /// <summary>
    /// Creates new topic attribute
    /// </summary>
    /// <param name="topic">Topic name</param>
    /// <param name="partition">Specific partition number</param>
    public TopicAttribute(string topic, int? partition)
    {
        Topic = topic;
        Partition = partition;
    }
}
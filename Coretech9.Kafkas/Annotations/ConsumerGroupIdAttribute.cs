namespace Coretech9.Kafkas.Annotations;

/// <summary>
/// Consumer Group Id for kafka consumer clients. Overrides default consumer group id.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class ConsumerGroupIdAttribute : Attribute
{
    /// <summary>
    /// Consumer Group Id Value
    /// </summary>
    public string ConsumerGroupId { get; set; }

    /// <summary>
    /// Creates new consumer group id attribute
    /// </summary>
    /// <param name="consumerGroupId">Consumer Group Id</param>
    public ConsumerGroupIdAttribute(string consumerGroupId)
    {
        ConsumerGroupId = consumerGroupId;
    }
}
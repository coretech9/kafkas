namespace Coretech9.Kafkas.Annotations;

/// <summary>
/// Wait strategy for retry operations
/// </summary>
public enum WaitStrategy
{
    /// <summary>
    /// Delay between each try is fixed and same amount of time
    /// </summary>
    Fixed,

    /// <summary>
    /// Before first retry waits x milliseconds. Before second, waits 2x milliseconds, third 3x milliseconds...
    /// </summary>
    Multiplier,

    /// <summary>
    /// Before first retry waits x milliseconds, Before second, waits x^2 milliseconds, third x^3 milliseconds...
    /// </summary>
    Exponental
}
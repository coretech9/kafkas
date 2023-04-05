using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Coretech9.Kafkas;

/// <summary>
/// Level level conversation extensions
/// </summary>
public static class LogExtensions
{
    /// <summary>
    /// Converts Confluent Log Level to Microsoft.Extensions.Logging.LogLevel
    /// </summary>
    public static LogLevel ToLogLevel(this SyslogLevel syslogLevel)
    {
        switch (syslogLevel)
        {
            case SyslogLevel.Emergency:
            case SyslogLevel.Critical:
            case SyslogLevel.Alert:
                return LogLevel.Critical;

            case SyslogLevel.Debug:
                return LogLevel.Debug;

            case SyslogLevel.Error:
                return LogLevel.Error;

            case SyslogLevel.Info:
                return LogLevel.Information;

            case SyslogLevel.Notice:
            case SyslogLevel.Warning:
                return LogLevel.Warning;

            default:
                return LogLevel.None;
        }
    }
}
using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace kafka.UnitTests.Helpers;

public class RecordingProducerHelper : DispatchProxy
{
    #region Properties

    #region Public

    #region DeliveryResult
    /// <summary>
    /// Gets or sets the delivery result of the produced message.
    /// </summary>
    public DeliveryResult<string, string>? DeliveryResult { get; set; }
    #endregion

    #region Exception
    /// <summary>
    /// Gets or sets the exception to be thrown when producing a message.
    /// </summary>
    public Exception? Exception { get; set; }
    #endregion

    #region Topic
    /// <summary>
    /// Gets the topic to which the message was produced.
    /// </summary>
    public string? Topic { get; private set; }
    #endregion

    #region Message
    /// <summary>
    /// Gets the message that was produced.
    /// </summary>
    public Message<string, string>? Message { get; private set; }
    #endregion

    #endregion

    #endregion

    #region Methods

    #region Protected

    #region Invoke
    /// <summary>
    /// Invokes the specified method on the proxy instance.
    /// </summary>
    /// <param name="targetMethod">The method to invoke.</param>
    /// <param name="args">The arguments to pass to the method.</param>
    /// <returns>The result of the method invocation.</returns>
    /// <exception cref="NotSupportedException">Thrown when the method is not supported.</exception>
    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        if (targetMethod?.Name == nameof(IProducer<string, string>.ProduceAsync))
        {
            Topic = Assert.IsType<string>(args![0]);
            Message = Assert.IsType<Message<string, string>>(args[1]);

            if (Exception is not null)
            {
                throw Exception;
            }

            return Task.FromResult(DeliveryResult!);
        }

        throw new NotSupportedException(targetMethod?.Name);
    }
    #endregion

    #endregion

    #endregion
}

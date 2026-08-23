namespace kafka.Api.Configuration;

public sealed class WorkerServicesOptions
{
    #region Properties

    #region Public

    #region SectionName
    /// <summary>
    /// Gets the name of the configuration section for worker services.
    /// </summary>
    public const string SectionName = "WorkerServices";
    #endregion

    #region AccountService
    /// <summary>
    /// Gets or sets the configuration options for the AccountService worker service.
    /// </summary>
    public WorkerServiceEndpointOptions AccountService { get; set; } = new();
    #endregion

    #region EmployeeService
    /// <summary>
    /// Gets or sets the configuration options for the EmployeeService worker service.
    /// </summary>
    public WorkerServiceEndpointOptions EmployeeService { get; set; } = new();
    #endregion

    #endregion

    #endregion
}
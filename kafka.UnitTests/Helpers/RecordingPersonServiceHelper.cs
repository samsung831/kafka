using kafka.Api.Services;
using kafka.Shared.Models.Responses;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.UnitTests.Helpers;

public sealed class RecordingPersonServiceHelper : IPersonService
{
    #region Properties

    #region Public

    #region PersonResult
    /// <summary>
    /// Gets or sets the result to be returned by the GetByGroupIdAsync method.
    /// </summary>
    public PersonResponseDto? PersonResult { get; init; }
    #endregion

    #region SearchResult
    /// <summary>
    /// Gets or sets the result to be returned by the SearchAsync method.
    /// </summary>
    public IReadOnlyCollection<PersonResponseDto> SearchResult { get; init; } = Array.Empty<PersonResponseDto>();
    #endregion

    #region GroupId
    /// <summary>
    /// Gets the group ID passed to the GetByGroupIdAsync method.
    /// </summary>
    public string? GroupId { get; private set; }
    #endregion

    #region FirstName
    /// <summary>
    /// Gets the first name passed to the SearchAsync method.
    /// </summary>
    public string? FirstName { get; private set; }
    #endregion

    #region LastName
    /// <summary>
    /// Gets the last name passed to the SearchAsync method.
    /// </summary>
    public string? LastName { get; private set; }
    #endregion

    #region IsActive
    /// <summary>
    /// Gets the isActive flag passed to the GetByGroupIdAsync and SearchAsync methods.
    /// </summary>
    public bool? IsActive { get; private set; }
    #endregion

    #region IsDeleted
    /// <summary>
    /// Gets the isDeleted flag passed to the GetByGroupIdAsync and SearchAsync methods.
    /// </summary>
    public bool? IsDeleted { get; private set; }
    #endregion

    #endregion

    #endregion

    #region Methods

    #region Public

    #region GetByGroupIdAsync
    /// <summary>
    /// Gets a person by group ID, recording the parameters passed and returning the predefined result.
    /// </summary>
    /// <param name="groupId">The group ID of the person to retrieve.</param>
    /// <param name="isActive">A flag indicating whether the person is active.</param>
    /// <param name="isDeleted">A flag indicating whether the person is deleted.</param>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>The predefined person result.</returns>
    public Task<PersonResponseDto?> GetByGroupIdAsync(string groupId, bool? isActive, bool? isDeleted, CancellationToken cancellationToken)
    {
        GroupId = groupId;
        IsActive = isActive;
        IsDeleted = isDeleted;
        return Task.FromResult(PersonResult);
    }
    #endregion

    #region SearchAsync
    /// <summary>
    /// Searches for persons based on the provided parameters, recording the parameters passed and returning the predefined search result.
    /// </summary>
    /// <param name="firstName">The first name of the person to search for.</param>
    /// <param name="lastName">The last name of the person to search for.</param>
    /// <param name="isActive">A flag indicating whether the person is active.</param>
    /// <param name="isDeleted">A flag indicating whether the person is deleted.</param>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>The predefined search result.</returns>
    public Task<IReadOnlyCollection<PersonResponseDto>> SearchAsync(string firstName, string lastName, bool? isActive, bool? isDeleted,
        CancellationToken cancellationToken)
    {
        FirstName = firstName;
        LastName = lastName;
        IsActive = isActive;
        IsDeleted = isDeleted;
        return Task.FromResult(SearchResult);
    }
    #endregion

    #endregion


    #endregion
}
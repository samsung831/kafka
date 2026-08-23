using kafka.Api.Services;
using kafka.Shared.Exceptions;
using kafka.Shared.Models.Responses;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;

namespace kafka.Api.Controllers;

[ApiController]
[Route("api/persons")]
[Produces("application/json")]
public sealed class PersonsController : ControllerBase
{
    #region Constructor
    public PersonsController(IPersonService personService)
    {
        _personService = personService;
    }
    #endregion

    #region Properties

    #region Private
    private readonly IPersonService _personService;
    #endregion

    #endregion

    #region Methods

    #region Public

    #region GetByGroupIdAsync
    /// <summary>
    /// Gets a person by their groupId.
    /// </summary>
    /// <param name="groupId">The groupId of the person to retrieve.</param>
    /// <param name="isActive">A flag indicating whether to filter by active status.</param>
    /// <param name="isDeleted">A flag indicating whether to filter by deleted status.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An IActionResult representing the result of the operation.</returns>
    /// <exception cref="RequestValidationException">Thrown when the request validation fails.</exception>
    /// <exception cref="ResourceNotFoundException">Thrown when the requested resource is not found.</exception>
    [HttpGet("{groupId}")]
    [ProducesResponseType(typeof(PersonResponseDto), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> GetByGroupIdAsync(string groupId, [FromQuery] bool? isActive, [FromQuery] bool? isDeleted,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(groupId))
        {
            throw new RequestValidationException("groupId is required.");
        }

        var person = await _personService.GetByGroupIdAsync(groupId.Trim(), isActive, isDeleted, cancellationToken);

        if (person is null)
        {
            throw new ResourceNotFoundException($"Person with groupId '{groupId}' was not found.");
        }

        return Ok(person);
    }
    #endregion

    #region SearchAsync
    /// <summary>
    /// Searches for persons based on the provided criteria.
    /// </summary>
    /// <param name="firstName">The first name of the person to search for.</param>
    /// <param name="lastName">The last name of the person to search for.</param>
    /// <param name="isActive">A flag indicating whether to filter by active status.</param>
    /// <param name="isDeleted">A flag indicating whether to filter by deleted status.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An IActionResult representing the result of the search operation.</returns>
    /// <exception cref="RequestValidationException">Thrown when the request validation fails.</exception>
    [HttpGet("search")]
    [ProducesResponseType(typeof(IReadOnlyCollection<PersonResponseDto>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ValidationProblemDetails), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> SearchAsync([FromQuery] string? firstName, [FromQuery] string? lastName,
        [FromQuery] bool? isActive, [FromQuery] bool? isDeleted, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(firstName))
        {
            throw new RequestValidationException("firstName is required.");
        }

        if (string.IsNullOrWhiteSpace(lastName))
        {
            throw new RequestValidationException("lastName is required.");
        }

        var persons = await _personService.SearchAsync(firstName.Trim(), lastName.Trim(), isActive, isDeleted, cancellationToken);

        return Ok(persons);
    }
    #endregion

    #endregion

    #endregion
}
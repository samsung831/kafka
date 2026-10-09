using kafka.Api.Controllers;
using kafka.Api.Services;
using kafka.Shared.Exceptions;
using kafka.Shared.Models.Responses;
using kafka.Shared.Models.Responses.Account;
using kafka.UnitTests.Helpers;
using Microsoft.AspNetCore.Mvc;

namespace kafka.UnitTests.Controllers;

public sealed class PersonsControllerTests
{
    #region Methods

    #region Private

    #region CreatePerson
    /// <summary>
    /// Creates a new instance of <see cref="PersonResponseDto"/> with an empty <see cref="AccountDto"/>.
    /// </summary>
    /// <returns>A new instance of <see cref="PersonResponseDto"/>.</returns>
    private static PersonResponseDto CreatePerson()
    {
        return new PersonResponseDto { Account = new AccountDto() };
    }
    #endregion

    #endregion

    #region Public

    #region GetByGroupIdAsync_TrimsInputAndReturnsPerson
    /// <summary>
    /// Tests that the GetByGroupIdAsync method trims the input groupId and returns the expected person.
    /// </summary>
    /// <param name="accountIsActive">The account active filter.</param>
    /// <param name="accountIsDeleted">The account deleted filter.</param>
    /// <param name="employmentIsActive">The employment active filter.</param>
    /// <param name="employmentIsDeleted">The employment deleted filter.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(null, null, null, null)]
    [InlineData(true, false, false, true)]
    [InlineData(false, true, true, false)]
    [InlineData(null, false, true, null)]
    [InlineData(false, null, null, true)]
    public async Task GetByGroupIdAsync_TrimsInputAndReturnsPerson(bool? accountIsActive, bool? accountIsDeleted,
        bool? employmentIsActive, bool? employmentIsDeleted)
    {
        var person = CreatePerson();
        var service = new RecordingPersonServiceHelper { PersonResult = person };
        var controller = new PersonsController(service);

        var result = await controller.GetByGroupIdAsync(" ABC123 ", accountIsActive, accountIsDeleted,
            employmentIsActive, employmentIsDeleted, CancellationToken.None);

        var ok = Assert.IsType<OkObjectResult>(result);
        Assert.Same(person, ok.Value);
        Assert.Equal("ABC123", service.GroupId);
        Assert.Equal(accountIsActive, service.AccountIsActive);
        Assert.Equal(accountIsDeleted, service.AccountIsDeleted);
        Assert.Equal(employmentIsActive, service.EmploymentIsActive);
        Assert.Equal(employmentIsDeleted, service.EmploymentIsDeleted);
    }
    #endregion

    #region GetByGroupIdAsync_WhenGroupIdIsMissing_ThrowsValidationException
    /// <summary>
    /// Tests that the GetByGroupIdAsync method throws a RequestValidationException when the groupId is missing or empty.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task GetByGroupIdAsync_WhenGroupIdIsMissing_ThrowsValidationException()
    {
        var controller = new PersonsController(new RecordingPersonServiceHelper());

        var exception = await Assert.ThrowsAsync<RequestValidationException>(
            () => controller.GetByGroupIdAsync(" ", null, null, null, null, CancellationToken.None));

        Assert.Equal("groupId is required.", exception.Message);
    }
    #endregion

    #region GetByGroupIdAsync_WhenPersonIsMissing_ThrowsNotFoundException
    /// <summary>
    /// Tests that the GetByGroupIdAsync method throws a ResourceNotFoundException when the person with the specified groupId is not found.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task GetByGroupIdAsync_WhenPersonIsMissing_ThrowsNotFoundException()
    {
        var controller = new PersonsController(new RecordingPersonServiceHelper());

        var exception = await Assert.ThrowsAsync<ResourceNotFoundException>(
            () => controller.GetByGroupIdAsync("ABC123", null, null, null, null, CancellationToken.None));

        Assert.Equal("Person with groupId 'ABC123' was not found.", exception.Message);
    }
    #endregion

    #region SearchAsync_ValidatesNamesAndReturnsDelegatedResults
    /// <summary>
    /// Tests that the SearchAsync method validates the firstName and lastName parameters,
    /// trims them, and returns the expected results from the delegated service.
    /// </summary>
    /// <param name="accountIsActive">The account active filter.</param>
    /// <param name="accountIsDeleted">The account deleted filter.</param>
    /// <param name="employmentIsActive">The employment active filter.</param>
    /// <param name="employmentIsDeleted">The employment deleted filter.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(null, null, null, null)]
    [InlineData(true, false, false, true)]
    [InlineData(false, true, true, false)]
    [InlineData(null, false, true, null)]
    [InlineData(false, null, null, true)]
    public async Task SearchAsync_ValidatesNamesAndReturnsDelegatedResults(bool? accountIsActive, bool? accountIsDeleted,
        bool? employmentIsActive, bool? employmentIsDeleted)
    {
        var persons = new[] { CreatePerson() };
        var service = new RecordingPersonServiceHelper { SearchResult = persons };
        var controller = new PersonsController(service);

        var result = await controller.SearchAsync(" Testo ", " Testic ", accountIsActive, accountIsDeleted,
            employmentIsActive, employmentIsDeleted, CancellationToken.None);

        var ok = Assert.IsType<OkObjectResult>(result);
        Assert.Same(persons, ok.Value);
        Assert.Equal("Testo", service.FirstName);
        Assert.Equal("Testic", service.LastName);
        Assert.Equal(accountIsActive, service.AccountIsActive);
        Assert.Equal(accountIsDeleted, service.AccountIsDeleted);
        Assert.Equal(employmentIsActive, service.EmploymentIsActive);
        Assert.Equal(employmentIsDeleted, service.EmploymentIsDeleted);

        await Assert.ThrowsAsync<RequestValidationException>(() => controller.SearchAsync(null, "Testic", null, null, null, null, CancellationToken.None));
        await Assert.ThrowsAsync<RequestValidationException>(() => controller.SearchAsync("Testo", " ", null, null, null, null, CancellationToken.None));
    }
    #endregion

    #endregion

    #endregion
}

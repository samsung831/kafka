using kafka.Shared.Models.Accounts;
using kafka.Shared.Models.Employees;
using kafka.Shared.Models.Responses;
using kafka.Shared.Models.Responses.Account;
using kafka.Shared.Models.Responses.Employee;
using kafka.Shared.MongoDB;
using MongoDB.Driver;

namespace kafka.Api.Services;

public sealed class PersonService : IPersonService
{
    #region Constructor
    public PersonService(MongoContext context)
    {
        _context = context;
    }
    #endregion

    #region Properties

    #region Private
    private readonly MongoContext _context;
    #endregion

    #endregion

    #region Methods

    #region Private

    #region MapAccount
    /// <summary>
    /// Maps an AccountDocument to an AccountDto, including nested properties such as Address, PersonalData, and EmployeeContact.
    /// </summary>
    /// <param name="account">The AccountDocument to map.</param>
    /// <returns>The mapped AccountDto.</returns>
    private AccountDto MapAccount(AccountDocument account)
    {
        return new AccountDto
        {
            Names = new Dictionary<string, object>(account.Names, account.Names.Comparer),
            Address = account.Address is { } address ? new AddressDto
            {
                Type = address.Type,
                Country = address.Country,
                State = address.State,
                City = address.City,
                ZipCode = address.ZipCode,
                AddressLine = address.AddressLine
            } : null,
            PersonalData = new PersonalDataDto
            {
                Age = account.PersonalData.Age,
                BirthDate = account.PersonalData.BirthDate,
                FirstName = account.PersonalData.FirstName,
                LastName = account.PersonalData.LastName,
                Gender = account.PersonalData.Gender
            },
            EmployeeContact = account.EmployeeContact is { } contact ? new AccountEmployeeContactDto
            {
                Private = contact.Private is { } privateContact ? new PrivateContactDto
                {
                    Email = privateContact.Email,
                    Mobile = privateContact.Mobile,
                    CountryCode = privateContact.CountryCode,
                    Country = privateContact.Country
                } : null
            } : null
        };
    }
    #endregion

    #region MapEmployee
    /// <summary>
    /// Maps an EmployeeDocument to an EmployeeDto, including nested properties such as EmploymentData and EmployeeContact.
    /// </summary>
    /// <param name="employee">The EmployeeDocument to map.</param>
    /// <returns>The mapped EmployeeDto.</returns>
    private EmployeeDto MapEmployee(EmployeeDocument employee)
    {
        return new EmployeeDto
        {
            EmploymentData = new EmploymentDataDto
            {
                EmploymentStatus = employee.EmploymentData.EmploymentStatus,
                OriginalHireDate = employee.EmploymentData.OriginalHireDate,
                LastHireDate = employee.EmploymentData.LastHireDate,
                LastJobPositionChangeDate = employee.EmploymentData.LastJobPositionChangeDate,
                ExpiredContractDate = employee.EmploymentData.ExpiredContractDate
            },
            EmployeeContact = employee.EmployeeContact is { } contact ? new EmployeeContactDto
            {
                Work = contact.Work is { } workContact ? new WorkContactDto
                {
                    Email = workContact.Email,
                    Mobile = workContact.Mobile
                } : null
            } : null
        };
    }
    #endregion

    #region CreateEmployeeGroupFilter
    /// <summary>
    /// Creates a filter for querying EmployeeDocument based on groupId, isActive, and isDeleted status.
    /// </summary>
    /// <param name="groupId">The group ID to filter by.</param>
    /// <param name="isActive">The active status to filter by.</param>
    /// <param name="isDeleted">The deleted status to filter by.</param>
    /// <returns>A filter definition for querying EmployeeDocument.</returns>
    private static FilterDefinition<EmployeeDocument> CreateEmployeeGroupFilter(string groupId, bool? isActive, bool? isDeleted)
    {
        var filters = new List<FilterDefinition<EmployeeDocument>>
        {
            Builders<EmployeeDocument>.Filter.Eq("mappingFields.EmployeeId.groupId", groupId)
        };

        AddStatusFilters(filters, isActive, isDeleted);

        return Builders<EmployeeDocument>.Filter.And(filters);
    }
    #endregion

    #region AddStatusFilters
    /// <summary>
    /// Adds status filters for isActive and isDeleted to the provided collection of filters.
    /// </summary>
    /// <typeparam name="TDocument">The type of the document.</typeparam>
    /// <param name="filters">The collection of filters to add to.</param>
    /// <param name="isActive">The active status to filter by.</param>
    /// <param name="isDeleted">The deleted status to filter by.</param>
    private static void AddStatusFilters<TDocument>(ICollection<FilterDefinition<TDocument>> filters, bool? isActive, bool? isDeleted)
    {
        if (isActive.HasValue)
        {
            filters.Add(Builders<TDocument>.Filter.Eq("isActive", isActive.Value));
        }

        if (isDeleted.HasValue)
        {
            filters.Add(Builders<TDocument>.Filter.Eq("isDeleted", isDeleted.Value));
        }
    }
    #endregion

    #endregion

    #region Public

    #region GetByGroupIdAsync
    /// <summary>
    /// Retrieves a PersonResponseDto by groupId, filtering employees by isActive and isDeleted status.
    /// </summary>
    /// <param name="groupId">The group ID to filter by.</param>
    /// <param name="isActive">The active status to filter by.</param>
    /// <param name="isDeleted">The deleted status to filter by.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns></returns>
    public async Task<PersonResponseDto?> GetByGroupIdAsync(string groupId, bool? isActive, bool? isDeleted, CancellationToken cancellationToken)
    {
        var accountFilter = Builders<AccountDocument>.Filter.Eq("mappingFields.EmployeeId.groupId", groupId);

        var account = await _context.Accounts.Find(accountFilter).FirstOrDefaultAsync(cancellationToken);

        if (account is null)
        {
            return null;
        }

        var employeeFilter = CreateEmployeeGroupFilter(groupId, isActive, isDeleted);

        var employees = await _context.Employees.Find(employeeFilter).ToListAsync(cancellationToken);

        return new PersonResponseDto
        {
            Account = MapAccount(account),
            Employees = employees.Select(MapEmployee).ToArray()
        };
    }
    #endregion

    #region SearchAsync
    /// <summary>
    /// Searches for PersonResponseDto objects based on first name and last name, filtering employees by optional isActive and isDeleted status.
    /// </summary>
    /// <param name="firstName">The first name to filter by.</param>
    /// <param name="lastName">The last name to filter by.</param>
    /// <param name="isActive">The active status to filter by.</param>
    /// <param name="isDeleted">The deleted status to filter by.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A collection of PersonResponseDto objects that match the specified filters.</returns>
    public async Task<IReadOnlyCollection<PersonResponseDto>> SearchAsync(string firstName, string lastName, bool? isActive, bool? isDeleted,
        CancellationToken cancellationToken)
    {
        var accountFilters = new List<FilterDefinition<AccountDocument>>
        {
            Builders<AccountDocument>.Filter.Eq("personalData.firstName", firstName),

            Builders<AccountDocument>.Filter.Eq("personalData.lastName", lastName)
        };

        var accounts = await _context.Accounts.Find(Builders<AccountDocument>.Filter.And(accountFilters)).ToListAsync(cancellationToken);

        if (accounts.Count == 0)
        {
            return Array.Empty<PersonResponseDto>();
        }

        var groupIds = accounts.Select(account => account.GroupId).Where(groupId => !string.IsNullOrWhiteSpace(groupId))
            .Distinct(StringComparer.Ordinal).ToArray();

        var employeeFilters =
            new List<FilterDefinition<EmployeeDocument>>
            {
                Builders<EmployeeDocument>.Filter.In("mappingFields.EmployeeId.groupId", groupIds)
            };

        AddStatusFilters(employeeFilters, isActive, isDeleted);

        var employees = await _context.Employees.Find(Builders<EmployeeDocument>.Filter.And(employeeFilters)).ToListAsync(cancellationToken);

        var employeesByGroupId = employees.GroupBy(employee => employee.GroupId).ToDictionary(
            group => group.Key,
            group => (IReadOnlyCollection<EmployeeDto>)group.Select(MapEmployee).ToArray(),
            StringComparer.Ordinal);

        return accounts.Select(account => new PersonResponseDto
            {
                Account = MapAccount(account),
                Employees = employeesByGroupId.TryGetValue(account.GroupId, out var matchingEmployees) ? matchingEmployees : Array.Empty<EmployeeDto>()
            }).ToArray();
    }
    #endregion

    #endregion

    #endregion
}
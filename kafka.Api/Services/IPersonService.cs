using kafka.Shared.Models.Responses;

namespace kafka.Api.Services;

public interface IPersonService
{
    Task<PersonResponseDto?> GetByGroupIdAsync(string groupId, bool? accountIsActive, bool? accountIsDeleted,
        bool? employmentIsActive, bool? employmentIsDeleted, CancellationToken cancellationToken);

    Task<IReadOnlyCollection<PersonResponseDto>> SearchAsync(string firstName, string lastName, bool? accountIsActive,
        bool? accountIsDeleted, bool? employmentIsActive, bool? employmentIsDeleted, CancellationToken cancellationToken);
}
using kafka.Shared.Models.Responses;

namespace kafka.Api.Services;

public interface IPersonService
{
    Task<PersonResponseDto?> GetByGroupIdAsync(string groupId, bool? isActive, bool? isDeleted, CancellationToken cancellationToken);

    Task<IReadOnlyCollection<PersonResponseDto>> SearchAsync(string firstName, string lastName, bool? isActive,
        bool? isDeleted, CancellationToken cancellationToken);
}